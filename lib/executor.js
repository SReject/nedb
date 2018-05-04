// dep modules
const async = require('async');

module.exports = class Executor {
    constructor() {
        this.buffer = [];
        this.ready = false;

        // This queue will execute all commands, one-by-one in order
        this.queue = async.queue((task, cb) => {
            let newArguments = [];

            // task.arguments is an array-like object on which adding a new field doesn't work, so we transform it into a real array
            for (let i = 0; i < task.arguments.length; i += 1) {
                newArguments.push(task.arguments[i]);
            }
            let lastArg = task.arguments[task.arguments.length - 1];

            // Always tell the queue task is complete. Execute callback if any was given.
            // Callback was supplied
            if (typeof lastArg === 'function') {

                newArguments[newArguments.length - 1] = (...args) => {

                    if (typeof setImmediate === 'function') {
                        setImmediate(cb);

                    } else {
                        process.nextTick(cb);
                    }
                    lastArg(...args);
                };

            // false/undefined/null supplied as callbback
            } else if (!lastArg && task.arguments.length !== 0) {
                newArguments[newArguments.length - 1] = cb;

            // Nothing supplied as callback
            } else {
                newArguments.push(cb);
            }

            task.fn.apply(task.this, newArguments);
        }, 1);
    }

    /**@desc Adds task to the queue, executes it if queue is empty
     * @param {Object} task.this
     * @param {Function} task.fn
     * @param {Array} task.arguments
     * @param {Boolean} forceQueuing Optional (defaults to false) force executor to queue task even if it is not ready
     */
    push(task, forceQueuing) {
        if (this.ready || forceQueuing) {
            this.queue.push(task);
        } else {
            this.buffer.push(task);
        }
    }

    /**@desc Queues all buffered tasks
     */
    processBuffer() {
        let i;
        this.ready = true;
        for (i = 0; i < this.buffer.length; i += 1) {
            this.queue.push(this.buffer[i]);
        }
        this.buffer = [];
    }
};
