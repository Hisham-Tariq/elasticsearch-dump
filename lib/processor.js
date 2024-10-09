const { EventEmitter } = require('events')
const { default: PQueue } = require('p-queue')
const delay = require('delay')
const vm = require('vm')
const path = require('path')
const fs = require('fs');
const { default: index } = require('async')

class TransportProcessor extends EventEmitter {
  log (message) {
    if (typeof this.options.logger === 'function') {
      this.options.logger(message)
    } else if (this.options.toLog === true) {
      this.emit('log', message)
    }
  }

  validateOptions (required = ['input', 'output']) {
    const validationErrors = []

    required.forEach(v => {
      if (!this.options[v]) {
        validationErrors.push(`\`${v}\` is a required input`)
      }
    })

    return validationErrors
  }

  static castArray (prop) {
    if (!(prop instanceof Array)) {
      return [prop]
    }
    return prop
  }

  generateModifiers (transforms) {
    return TransportProcessor.castArray(transforms).map(transform => {
      if (transform[0] === '@') {
        return doc => {
          const filePath = transform.slice(1).split('?')
          const resolvedFilePath = path.resolve(process.cwd(), filePath[0])
          return require(resolvedFilePath)(doc, TransportProcessor.getParams(filePath[1]))
        }
      } else {
        const modificationScriptText = `(function(doc) { ${transform} })`
        return new vm.Script(modificationScriptText).runInThisContext()
      }
    })
  }

  applyModifiers (data = [], modifiers = this.modifiers) {
    if (modifiers.length && data.length) {
      for (let i = 0; i < data.length; i++) {
        modifiers.forEach(modifier => {
          modifier(data[i])
        })
      }
    }
  }

  static getParams (query) {
    if (!query) {
      return {}
    }

    return (/^[?#]/.test(query) ? query.slice(1) : query)
      .split('&')
      .reduce((params, param) => {
        const [key, value] = param.split('=')
        params[key] = value ? decodeURIComponent(value.replace(/\+/g, ' ')) : ''
        return params
      }, {})
  }

  __update_checkpoint (offset, status) {
    const timestamp = Date.now()
    let indexName = 'unknown'
    if (this.options.inputType === 'file') {
      // find the last / in the path
      const lastSlash = this.options.input.lastIndexOf('/')
      // find the last . in the path
      const lastDot = this.options.input.lastIndexOf('.')
      // get the file name
      index = this.options.input.substring(lastSlash + 1, lastDot)
    } else {
      indexName = this.options.inputBase.index
    }

    const checkpointData = {
      timestamp,
      indexName,
      status,
      offset
    }
    if (status === 'error' || status === 'complete') {  
      console.table(checkpointData)
    }

    const checkpointPath = path.resolve(this.options.checkpointPath || './checkpoint.json');

    try {
      fs.writeFileSync(checkpointPath, JSON.stringify(checkpointData, null, 4));
      this.log(`Checkpoint updated: ${JSON.stringify(checkpointData)}`);
    } catch (err) {
      this.emit('error', err);
      this.log(`Failed to update checkpoint: ${String(err)}`);
    }
  }

  async _loop (limit, offset, totalWrites) {
    const queue = new PQueue({
      concurrency: this.options.concurrency || Infinity,
      interval: this.options.concurrencyInterval || 0,
      intervalCap: this.options.intervalCap || Infinity,
      carryoverConcurrencyCount: this.options.carryoverConcurrencyCount || false
    })
    return this.__looper(limit, offset, totalWrites, queue)
      .then(totalWrites => {
        this.log(`Total Writes: ${totalWrites}`)
        this.log('dump complete')
        this.__update_checkpoint(totalWrites, 'complete')
        return totalWrites
      })
      .catch(err => {
        this.emit('error', err)
        this.log(`Total Writes: ${totalWrites}`)
        this.log(`dump ended with error (get phase) => ${String(err)}`)
        this.__update_checkpoint(totalWrites, 'error')
        throw err
      })
  }

  

  async __looper (limit, offset, totalWrites, queue) {
    const ignoreErrors = this.options['ignore-errors'] === true

    return new Promise((resolve, reject) => {
      this.input.get(limit, offset, (err, data) => {
        if (err) {
          this.emit('error', err)
          if (!ignoreErrors) {
            return reject(err)
          }
        }

        this.log(`got ${data.length} objects from source ${this.inputType} (offset: ${offset})`)
        this.__update_checkpoint(offset, 'in-progress')
        this.applyModifiers(data)

        const overlappedIoPromise = this.set(data, limit, offset)
          .then(writes => {
            totalWrites += writes
            if (data.length > 0) {
              this.log(`sent ${data.length} objects to destination ${this.outputType}, wrote ${writes}`)
            }
          })
          .catch(err => {
            if (ignoreErrors) {
              return Promise.resolve()
            }

            this.emit('error', err)
            return Promise.reject(err)
          })

        if (data.length === 0) {
          return queue.onIdle()
            .then(() => resolve(totalWrites))
            .catch(reject)
        } else {
          return queue.add(() => overlappedIoPromise)
            .then(() => {
              offset += data.length
              return delay(this.options.throttleInterval || 0)
                .then(() => {
                  return this.__looper(limit, offset, totalWrites, queue)
                    .then(resolve)
                })
            })
            .catch(reject)
        }
      })
    })
  }
}

module.exports = TransportProcessor
