// dep modules
const async = require('async');

module.exports = class Executor {
    constructor() {
        this.buffer = [];
        this.ready = false;

        // This queue will execute all commands, one-by-one in order
        this.queue = async.queue((task, cb) => {

            // Ensure task.arguments is an array
            let taskArgs = task.arguments;
            if (taskArgs == null) {
                taskArgs = [];

            } else if (Array.isArray(taskArgs) || Object.prototype.toString.call(taskArgs) === '[object Arguments]') {
                taskArgs = Array.from(taskArgs);

            } else {
                taskArgs = [taskArgs];
            }

            // task.arguments is an array-like object on which adding a new field doesn't work, so we transform it into a real array
            let lastArg = taskArgs[taskArgs.length - 1];

            // Callback was supplied
            if (typeof lastArg === 'function') {

                // Always tell the queue task is complete. Execute callback if any was given.
                taskArgs[taskArgs.length - 1] = (...args) => {
                    if (typeof setImmediate === 'function') {
                        setImmediate(cb);
                    } else {
                        process.nextTick(cb);
                    }
                    lastArg(...args);
                };
            } else if (!lastArg && taskArgs.length !== 0) {
                taskArgs[taskArgs.length - 1] = cb;
            } else {
                taskArgs.push(cb);
            }

            task.fn.apply(task.this, taskArgs);
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
