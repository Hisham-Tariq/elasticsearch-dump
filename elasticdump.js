const http = require('http')
const https = require('https')
const TransportProcessor = require('./lib/processor')
const { promisify } = require('util')
const ioHelper = require('./lib/ioHelper')
const _ = require('lodash')
const parseBaseURL = require('./lib/parse-base-url')
const { type } = require('os')

class ElasticDump extends TransportProcessor {
  constructor (input, output, options) {
    super()
    if (arguments.length === 1 && _.isPlainObject(arguments[0])) {
      options = input
      output = options.output
      input = options.input
    }
    this.input = input
    this.output = output
    this.options = options
    this.modifiers = []
    this.sModififiers = []

    if (output !== '$' && (this.options.toLog === null || this.options.toLog === undefined)) {
      this.options.toLog = true
    }

    this.validationErrors = this.validateOptions()

    if (this.options.searchBodyTemplate) {
      this.sModififiers = this.generateModifiers(this.options.searchBodyTemplate)
      const state = {}
      this.applyModifiers([state], this.sModififiers)
      this.options.searchBody = state.searchBody
    }

    if (options.maxSockets) {
      this.log(`globally setting maxSockets=${options.maxSockets}`)
      http.globalAgent.maxSockets = options.maxSockets
      https.globalAgent.maxSockets = options.maxSockets
    }

    ioHelper(this, 'input')
    ioHelper(this, 'output')
    if (this.options.mode == 'backup'){
      this.options.inputBase = parseBaseURL(this.options.input, this.options)
      this.options.inputType = "elastic"
      // either output is a file or an elasticserver
      console.log(typeof this.options.output)
      if (this.options.output.includes('http') || this.options.output.includes('https')){
        this.options.outputBase = parseBaseURL(this.options.output, this.options)
        this.options.outputType = "elastic"
      } else {
        this.options.outputBase = parseBaseURL(this.options.output, this.options)
        this.options.outputType = "file"
      }
    } else if (this.options.mode == 'restore'){
      this.inputType = "file"
      this.outputType = "elastic"
      this.inputBase = parseBaseURL(this.options.input, this.options)
      this.outputBase = parseBaseURL(this.options.output, this.options)
    }  else {
      // exit with error
      this.emit('error', { errors: ['invalid mode, mode can only be either backup or restore'] })
    }

    if (this.options.type === 'data' && this.options.transform) {
      this.modifiers = this.generateModifiers(this.options.transform)
    }
  }

  dump (callback, continuing, limit, offset, totalWrites) {
    if (this.validationErrors.length > 0) {
      this.emit('error', { errors: this.validationErrors })
      callback(new Error('There was an error starting this dump'))
      return
    }
    // console.log(`Easlticdump.js > dump > output ${this.output}`)
    // console.log(`Checkpoints will be managed here ${this.options.checkpointPath}`)

    // promisify helpers
    this.get = promisify(this.output.get).bind(this.input)
    this.set = promisify(this.output.set).bind(this.output)

    if (!limit) { limit = this.options.limit }
    if (!offset) { offset = this.options.offset }
    if (!totalWrites) { totalWrites = 0 }

    if (continuing !== true) {
      this.log('starting dump')

      if (this.options.offset) {
        this.log(`Warning: offsetting ${this.options.offset} rows.`)
        this.log('  * Using an offset doesn\'t guarantee that the offset rows have already been written, please refer to the HELP text.')
      }
      if (this.modifiers.length) {
        this.log(`Will modify documents using these scripts: ${this.options.transform}`)
      }
    }

    this._loop(limit, offset, totalWrites)
      .then((totalWrites) => {
        if (typeof callback === 'function') { return callback(null, totalWrites) }
      }, (error) => {
        if (typeof callback === 'function') { return callback(error/*, totalWrites */) }
      })
  }
}

module.exports = ElasticDump
