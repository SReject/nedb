// dep modules
const _ = require('underscore');

// lib modules
const model = require('./model');



class Cursor {

    /**@desc Create a new cursor for this collection
     * @param {Datastore} db - The datastore this cursor is bound to
     * @param {Query} query - The query this cursor will operate on
     * @param {Function} execFn - Handler to be executed after cursor has found the results and before the callback passed to find/findOne/update/remove
     */
    constructor(db, query, execFn) {
        this.db = db;
        this.query = query || {};
        if (execFn) {
            this.execFn = execFn;
        }
    }

    /**@desc Set a limit to the number of results
     * @param {Number} count - Max number of results
     * @returns {Cursor}
     */
    limit(count) {
        this._limit = count;
        return this;
    }

    /**@desc Set a number of items to skip
     * @param {Number} count - Number of items to skip
     * @returns {Cursor}
     */
    skip(count) {
        this._skip = count;
        return this;
    }

    /**@desc Sort results of the query
     * @param {Object} filter - Filter is { field: order }, field can use the dot-notation, order is 1 for ascending and -1 for descending
     * @returns {Cursor}
     */
    sort(filter) {
        this._sort = filter;
        return this;
    }

    /**@desc Add the use of a projection
     * @param {Object} projection - MongoDB-style projection. {} means take all fields. Then it's { key1: 1, key2: 1 } to take only key1 and key2
     *                              { key1: 0, key2: 0 } to omit only key1 and key2. Except _id, you can't mix takes and omits
     * @returns {Cursor}
     */
    projection(projection) {
        this._projection = projection;
        return this;
    }

    /**@desc Apply projections to a collection of documents
     * @param {Object} canidates - Documents to be filtered
     * @returns {Object}
     */
    project(candidates) {
        let res = [],
            self = this,
            keepId,
            action,
            keys;

        if (this._projection === undefined || Object.keys(this._projection).length === 0) {
            return candidates;
        }

        keepId = this._projection._id === 0 ? false : true;
        this._projection = _.omit(this._projection, '_id');

        // Check for consistency
        keys = Object.keys(this._projection);
        keys.forEach(key => {
            if (action !== undefined && self._projection[key] !== action) {
                throw new Error("Can't both keep and omit fields except for _id");
            }
            action = self._projection[key];
        });

        // Do the actual projection
        candidates.forEach(candidate => {
            let toPush;
            if (action === 1) { // pick-type projection
                toPush = { $set: {} };
                keys.forEach(key => {
                    toPush.$set[key] = model.getDotValue(candidate, key);
                    if (toPush.$set[key] === undefined) {
                        delete toPush.$set[key];
                    }
                });
                toPush = model.modify({}, toPush);

            } else { // omit-type projection
                toPush = { $unset: {} };
                keys.forEach(key => {
                    toPush.$unset[key] = true;
                });
                toPush = model.modify(candidate, toPush);
            }
            if (keepId) {
                toPush._id = candidate._id;
            } else {
                delete toPush._id;
            }
            res.push(toPush);
        });

        return res;
    }

    /**@desc executes a lookup returning pointers to the documents
     * @param {Function} _callback - Signature: err, results
     * @returns {Any}
     */
    _exec(_callback) {
        let res = [],
            added = 0,
            skipped = 0,
            self = this,
            error = null,
            i,
            keys;

        function callback (error, res) {
            if (self.execFn) {
                return self.execFn(error, res, _callback);
            }
            return _callback(error, res);
        }

        this.db.getCandidates(this.query, (err, candidates) => {
            if (err) {
                return callback(err);
            }

            try {
                for (i = 0; i < candidates.length; i += 1) {
                    if (model.match(candidates[i], self.query)) {
                        // If a sort is defined, wait for the results to be sorted before applying limit and skip
                        if (!self._sort) {
                            if (self._skip && self._skip > skipped) {
                                skipped += 1;
                            } else {
                                res.push(candidates[i]);
                                added += 1;
                                if (self._limit && self._limit <= added) {
                                    break;
                                }
                            }
                        } else {
                            res.push(candidates[i]);
                        }
                    }
                }
            } catch (err) {
                return callback(err);
            }

            // Apply all sorts
            if (self._sort) {
                keys = Object.keys(self._sort);

                let criteria = [];
                keys.forEach(key => {
                    criteria.push({key: key, direction: self._sort[key]});
                });

                res.sort((a, b) => {
                    let criterion,
                        compare,
                        i = 0;

                    // TODO: improve this with .forEach
                    for (; i < criteria.length; i++) {
                        criterion = criteria[i];
                        compare = criterion.direction * model.compareThings(model.getDotValue(a, criterion.key), model.getDotValue(b, criterion.key), self.db.compareStrings);
                        if (compare !== 0) {
                            return compare;
                        }
                    }
                    return 0;
                });

                // Applying limit and skip
                let limit = self._limit || res.length,
                    skip = self._skip || 0;

                res = res.slice(skip, skip + limit);
            }

            // Apply projection
            try {
                res = self.project(res);
            } catch (e) {
                error = e;
                res = undefined;
            }

            return callback(error, res);
        });
    }

    /**@desc Executes a query
     */
    exec() {
        this.db.executor.push({ this: this, fn: this._exec, arguments: arguments });
    }
}


// Interface
module.exports = Cursor;
