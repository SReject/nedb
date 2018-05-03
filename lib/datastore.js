// native modules
const util = require('util');
const Emitter = require('events').EventEmitter;

// installed modules
const async = require('async');
const _ = require('underscore');

// project modules
let customUtils = require('./customUtils'),
    model = require('./model'),
    Executor = require('./executor'),
    Index = require('./indexes'),
    Persistence = require('./persistence'),
    Cursor = require('./cursor');


module.exports = class Datastore extends Emitter {

    /**@desc Create a new collection
     * @constructor
     * @param {String} options.filename Optional, datastore will be in-memory only if not provided
     * @param {Boolean} options.timestampData Optional, defaults to false. If set to true, createdAt and updatedAt will be created and populated automatically (if not specified by user)
     * @param {Boolean} options.inMemoryOnly Optional, defaults to false
     * @param {String} options.nodeWebkitAppName Optional, specify the name of your NW app if you want options.filename to be relative to the directory where
     *                                            Node Webkit stores application data such as cookies and local storage (the best place to store data in my opinion)
     * @param {Boolean} options.autoload Optional, defaults to false
     * @param {Function} options.onload Optional, if autoload is used this will be called after the load database with the error object as parameter. If you don't pass it the error will be thrown
     * @param {Function} options.afterSerialization/options.beforeDeserialization Optional, serialization hooks
     * @param {Number} options.corruptAlertThreshold Optional, threshold after which an alert is thrown if too much data is corrupt
     * @param {Function} options.compareStrings Optional, string comparison function that overrides default for sorting
     *
     * @event Datastore#compaction.done
     */
    constructor(options) {
        super();
        let filename;

        // Retrocompatibility with v0.6 and before
        if (typeof options === 'string') {
            filename = options;
            this.inMemoryOnly = false; // Default
        } else {
            options = options || {};
            filename = options.filename;
            this.inMemoryOnly = options.inMemoryOnly || false;
            this.autoload = options.autoload || false;
            this.timestampData = options.timestampData || false;
        }

        // Determine whether in memory or persistent
        if (!filename || typeof filename !== 'string' || filename.length === 0) {
            this.filename = null;
            this.inMemoryOnly = true;
        } else {
            this.filename = filename;
        }

        // String comparison function
        this.compareStrings = options.compareStrings;

        // Persistence handling
        this.persistence = new Persistence({
            db: this,
            nodeWebkitAppName: options.nodeWebkitAppName,
            afterSerialization: options.afterSerialization,
            beforeDeserialization: options.beforeDeserialization,
            corruptAlertThreshold: options.corruptAlertThreshold
        });

        // This new executor is ready if we don't use persistence
        // If we do, it will only be ready once loadDatabase is called
        this.executor = new Executor();
        if (this.inMemoryOnly) {
            this.executor.ready = true;
        }

        // Indexed by field name, dot notation can be used
        // _id is always indexed and since _ids are generated randomly the underlying
        // binary is always well-balanced
        this.indexes = {};
        this.indexes._id = new Index({ fieldName: '_id', unique: true });
        this.ttlIndexes = {};

        // Queue a load of the database right away and call the onload handler
        // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
        if (this.autoload) {
            this.loadDatabase(options.onload || (err => {
                if (err) {
                    throw err;
                }
            }));
        }
    }

    /**@desc Loads the database
     */
    loadDatabase() {
        this.executor.push({
            this: this.persistence,
            fn: this.persistence.loadDatabase,
            arguments: arguments
        }, true);
    }

    /**@desc Get an array of all the data in the database
     */
    getAllData() {
        return this.indexes._id.getAll();
    }

    /**@desc Reset all currently defined indexes
     */
    resetIndexes(newData) {
        let self = this;

        Object.keys(this.indexes).forEach(function (i) {
            self.indexes[i].reset(newData);
        });
    }

    /**@desc Ensure an index is kept for this field. Same parameters as lib/indexes
     * @todo For now this function is synchronous, we need to test how much time it takes
     * @param {String} options.fieldName
     * @param {Boolean} options.unique
     * @param {Boolean} options.sparse
     * @param {Number} [options.expireAfterSeconds] - Optional, if set this index becomes a TTL index (only works on Date fields, not arrays of Date)
     * @param {Function} [cb] Optional callback, signature: err
     */
    ensureIndex(options, callback = () => {}) {
        let err;

        options = options || {};

        if (!options.fieldName) {
            err = new Error("Cannot create an index without a fieldName");
            err.missingFieldName = true;
            return callback(err);
        }
        if (this.indexes[options.fieldName]) {
            return callback(null);
        }

        this.indexes[options.fieldName] = new Index(options);
        if (options.expireAfterSeconds !== undefined) {
            this.ttlIndexes[options.fieldName] = options.expireAfterSeconds;
        } // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

        try {
            this.indexes[options.fieldName].insert(this.getAllData());
        } catch (e) {
            delete this.indexes[options.fieldName];
            return callback(e);
        }

        // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
        this.persistence.persistNewState([{ $$indexCreated: options }], callback);
    }

    /**@desc Remove an index
     * @param {String} fieldName
     * @param {Function} callback signature: err
     */
    removeIndex(fieldName, callback = () => {}) {
        delete this.indexes[fieldName];
        this.persistence.persistNewState([{ $$indexRemoved: fieldName }], callback);
    }

    /**@desc Add one or several document(s) to all indexes
     * @param {Document} doc
     */
    addToIndexes(doc) {
        let i, failingIndex, error,
            keys = Object.keys(this.indexes);
        for (i = 0; i < keys.length; i += 1) {
            try {
                this.indexes[keys[i]].insert(doc);
            } catch (e) {
                failingIndex = i;
                error = e;
                break;
            }
        }

        // If an error happened, we need to rollback the insert on all other indexes
        if (error) {
            for (i = 0; i < failingIndex; i += 1) {
                this.indexes[keys[i]].remove(doc);
            }

            throw error;
        }
    }

    /**@desc Remove one or several document(s) from all indexes
     * @param {Document} doc
     */
    removeFromIndexes(doc) {
        let self = this;

        Object.keys(this.indexes).forEach(i => self.indexes[i].remove(doc));
    }

    /**@desc Update one or several documents in all indexes
     * @param {Object|Array} oldDoc
     * @param {Object} [newDoc]
     */
    updateIndexes(oldDoc, newDoc) {
        let i, failingIndex, error,
            keys = Object.keys(this.indexes);
        for (i = 0; i < keys.length; i += 1) {
            try {
                this.indexes[keys[i]].update(oldDoc, newDoc);
            } catch (e) {
                failingIndex = i;
                error = e;
                break;
            }
        }

        // If an error happened, we need to rollback the update on all other indexes
        if (error) {
            for (i = 0; i < failingIndex; i += 1) {
                this.indexes[keys[i]].revertUpdate(oldDoc, newDoc);
            }

            throw error;
        }
    }

    /**@desc Return the list of candidates for a given query
     * @param {Query} query
     * @param {Boolean} dontExpireStaleDocs Optional, defaults to false, if true don't remove stale docs. Useful for the remove function which shouldn't be impacted by expirations
     * @param {Function} callback Signature err, candidates
     */
    getCandidates(query, dontExpireStaleDocs, callback) {
        let indexNames = Object.keys(this.indexes),
            self = this,
            usableQueryKeys;

        if (typeof dontExpireStaleDocs === 'function') {
            callback = dontExpireStaleDocs;
            dontExpireStaleDocs = false;
        }


        async.waterfall([
            // STEP 1: get candidates list by checking indexes from most to least frequent usecase
            cb => {
                // For a basic match
                usableQueryKeys = [];
                Object.keys(query).forEach(function (k) {
                    if (typeof query[k] === 'string' || typeof query[k] === 'number' || typeof query[k] === 'boolean' || util.isDate(query[k]) || query[k] === null) {
                        usableQueryKeys.push(k);
                    }
                });
                usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
                if (usableQueryKeys.length > 0) {
                    return cb(null, self.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]]));
                }

                // For a $in match
                usableQueryKeys = [];
                Object.keys(query).forEach(function (k) {
                    if (query[k] && query[k].hasOwnProperty('$in')) {
                        usableQueryKeys.push(k);
                    }
                });
                usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
                if (usableQueryKeys.length > 0) {
                    return cb(null, self.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]].$in));
                }

                // For a comparison match
                usableQueryKeys = [];
                Object.keys(query).forEach(function (k) {
                    if (query[k] && (query[k].hasOwnProperty('$lt') || query[k].hasOwnProperty('$lte') || query[k].hasOwnProperty('$gt') || query[k].hasOwnProperty('$gte'))) {
                        usableQueryKeys.push(k);
                    }
                });
                usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
                if (usableQueryKeys.length > 0) {
                    return cb(null, self.indexes[usableQueryKeys[0]].getBetweenBounds(query[usableQueryKeys[0]]));
                }

                // By default, return all the DB data
                return cb(null, self.getAllData());
            },
            // STEP 2: remove all expired documents
            docs => {
                if (dontExpireStaleDocs) {
                    return callback(null, docs);
                }

                let expiredDocsIds = [],
                    validDocs = [],
                    ttlIndexesFieldNames = Object.keys(self.ttlIndexes);

                docs.forEach(doc => {
                    let valid = true;
                    ttlIndexesFieldNames.forEach(function (i) {
                        if (doc[i] !== undefined && util.isDate(doc[i]) && Date.now() > doc[i].getTime() + self.ttlIndexes[i] * 1000) {
                            valid = false;
                        }
                    });
                    if (valid) {
                        validDocs.push(doc);
                    } else {
                        expiredDocsIds.push(doc._id);
                    }
                });

                async.eachSeries(
                    expiredDocsIds,
                    (_id, cb) => {
                        self._remove({ _id: _id }, {}, err => {
                            if (err) {
                                return callback(err);
                            }
                            return cb();
                        });
                    },
                    () => callback(null, validDocs)
                );
            }
        ]);
    }

    /**@desc Insert a new document
     * @private
     * @param {Document} newDoc
     * @param {Function} [cb] signature: err, insertedDoc
     */
    _insert(newDoc, cb) {
        let callback = cb || function () {},
            preparedDoc;
        try {
            preparedDoc = this.prepareDocumentForInsertion(newDoc);
            this._insertInCache(preparedDoc);
        } catch (e) {
            return callback(e);
        }

        this.persistence.persistNewState(util.isArray(preparedDoc) ? preparedDoc : [preparedDoc], err => {
            if (err) {
                return callback(err);
            }
            return callback(null, model.deepCopy(preparedDoc));
        });
    }

    /**@desc Create a new _id that's not already in use
     */
    createNewId() {
        let tentativeId = customUtils.uid(16);
        // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
        if (this.indexes._id.getMatching(tentativeId).length > 0) {
            tentativeId = this.createNewId();
        }
        return tentativeId;
    }

    /**@desc Prepare a document (or array of documents) to be inserted in a database
     * @private
     * @param {Document} newDoc
     * @returns {PreparedDocument}
     */
    prepareDocumentForInsertion(newDoc) {
        let preparedDoc, self = this;

        if (util.isArray(newDoc)) {
            preparedDoc = [];
            newDoc.forEach(function (doc) {
                preparedDoc.push(self.prepareDocumentForInsertion(doc));
            });

        } else {
            preparedDoc = model.deepCopy(newDoc);
            if (preparedDoc._id === undefined) {
                preparedDoc._id = this.createNewId();
            }
            let now = new Date();
            if (this.timestampData && preparedDoc.createdAt === undefined) {
                preparedDoc.createdAt = now;
            }
            if (this.timestampData && preparedDoc.updatedAt === undefined) {
                preparedDoc.updatedAt = now;
            }
            model.checkObject(preparedDoc);
        }
        return preparedDoc;
    }

    /**@desc If newDoc is an array of documents, this will insert all documents in the cache
     * @private
     * @param {PreparedDocument} preparedDoc
     */
    _insertInCache(preparedDoc) {
        if (util.isArray(preparedDoc)) {
            this._insertMultipleDocsInCache(preparedDoc);
        } else {
            this.addToIndexes(preparedDoc);
        }
    }

    /**@desc Inserts multiple prepared documents
     * @private
     */
    _insertMultipleDocsInCache(preparedDocs) {
        let i, failingI, error;

        for (i = 0; i < preparedDocs.length; i += 1) {
            try {
                this.addToIndexes(preparedDocs[i]);
            } catch (e) {
                error = e;
                failingI = i;
                break;
            }
        }

        if (error) {
            for (i = 0; i < failingI; i += 1) {
                this.removeFromIndexes(preparedDocs[i]);
            }

            throw error;
        }
    }

    /**@desc Inserts a document into the database
     */
    insert() {
        this.executor.push({ this: this, fn: this._insert, arguments: arguments });
    }

    /**@desc Count all documents matching the query
     * @param {Object} query MongoDB-style query
     * @param {Function} [callback]
     */
    count(query, callback) {
        let cursor = new Cursor(this, query, function(err, docs, callback) {
            if (err) {
                return callback(err);
            }
            return callback(null, docs.length);
        });

        if (typeof callback === 'function') {
            cursor.exec(callback);
        } else {
            return cursor;
        }
    }

    /**@desc Find all documents matching the query
     * @param {Object} query MongoDB-style query
     * @param {Object} projection MongoDB-style projection
     * @returns {Cursor|Document}
     */
    find(query, projection, callback) {
        switch (arguments.length) {
        case 1:
            projection = {};
            // callback is undefined, will return a cursor
            break;
        case 2:
            if (typeof projection === 'function') {
                callback = projection;
                projection = {};
            } // If not assume projection is an object and callback undefined
            break;
        }

        let cursor = new Cursor(this, query, function(err, docs, callback) {
            let res = [], i;

            if (err) {
                return callback(err);
            }

            for (i = 0; i < docs.length; i += 1) {
                res.push(model.deepCopy(docs[i]));
            }
            return callback(null, res);
        });

        cursor.projection(projection);
        if (typeof callback === 'function') {
            cursor.exec(callback);
        } else {
            return cursor;
        }
    }

    /**@desc Find one document matching the query
     * @param {Object} query MongoDB-style query
     * @param {Object} projection MongoDB-style projection
     * @returns {Cursor|Document}
     */
    findOne(query, projection, callback) {
        switch (arguments.length) {
        case 1:
            projection = {};
            // callback is undefined, will return a cursor
            break;
        case 2:
            if (typeof projection === 'function') {
                callback = projection;
                projection = {};
            } // If not assume projection is an object and callback undefined
            break;
        }

        let cursor = new Cursor(this, query, function(err, docs, callback) {
            if (err) {
                return callback(err);
            }
            if (docs.length === 1) {
                return callback(null, model.deepCopy(docs[0]));
            }
            return callback(null, null);

        });

        cursor.projection(projection).limit(1);
        if (typeof callback === 'function') {
            cursor.exec(callback);
        } else {
            return cursor;
        }
    }

    /**@desc Updates all matching documents
     * @private
     * @param {Object} query
     * @param {Object} updateQuery
     * @param {Boolean} [options.multi=false]
     * @param {Boolean} [options.upsert=false]
     * @param {Boolean} [options.returnUpdatedDocs=false]
     * @param {Function} [cb]
     */
    _update(query, updateQuery, options, cb) {
        let callback,
            self = this,
            numReplaced = 0,
            multi, upsert,
            i;
        if (typeof options === 'function') {
            cb = options; options = {};
        }
        callback = cb || function () {};
        multi = options.multi !== undefined ? options.multi : false;
        upsert = options.upsert !== undefined ? options.upsert : false;

        async.waterfall([
            function (cb) { // If upsert option is set, check whether we need to insert the doc
                if (!upsert) {
                    return cb();
                }

                // Need to use an internal function not tied to the executor to avoid deadlock
                let cursor = new Cursor(self, query);
                cursor.limit(1)._exec(function (err, docs) {
                    if (err) {
                        return callback(err);
                    }
                    if (docs.length === 1) {
                        return cb();
                    }
                    let toBeInserted;

                    try {
                        model.checkObject(updateQuery);
                        // updateQuery is a simple object with no modifier, use it as the document to insert
                        toBeInserted = updateQuery;
                    } catch (e) {
                        // updateQuery contains modifiers, use the find query as the base,
                        // strip it from all operators and update it according to updateQuery
                        try {
                            toBeInserted = model.modify(model.deepCopy(query, true), updateQuery);
                        } catch (err) {
                            return callback(err);
                        }
                    }

                    return self._insert(toBeInserted, function (err, newDoc) {
                        if (err) {
                            return callback(err);
                        }
                        return callback(null, 1, newDoc, true);
                    });

                });
            },
            function () { // Perform the update
                let modifiedDoc, modifications = [], createdAt;

                self.getCandidates(query, function (err, candidates) {
                    if (err) {
                        return callback(err);
                    }

                    // Preparing update (if an error is thrown here neither the datafile nor
                    // the in-memory indexes are affected)
                    try {
                        for (i = 0; i < candidates.length; i += 1) {
                            if (model.match(candidates[i], query) && (multi || numReplaced === 0)) {
                                numReplaced += 1;
                                if (self.timestampData) {
                                    createdAt = candidates[i].createdAt;
                                }
                                modifiedDoc = model.modify(candidates[i], updateQuery);
                                if (self.timestampData) {
                                    modifiedDoc.createdAt = createdAt;
                                    modifiedDoc.updatedAt = new Date();
                                }
                                modifications.push({ oldDoc: candidates[i], newDoc: modifiedDoc });
                            }
                        }
                    } catch (err) {
                        return callback(err);
                    }

                    // Change the docs in memory
                    try {
                        self.updateIndexes(modifications);
                    } catch (err) {
                        return callback(err);
                    }

                    // Update the datafile
                    let updatedDocs = _.pluck(modifications, 'newDoc');
                    self.persistence.persistNewState(updatedDocs, function (err) {
                        if (err) {
                            return callback(err);
                        }
                        if (!options.returnUpdatedDocs) {
                            return callback(null, numReplaced);
                        }
                        let updatedDocsDC = [];
                        updatedDocs.forEach(function (doc) {
                            updatedDocsDC.push(model.deepCopy(doc));
                        });
                        if (!multi) {
                            updatedDocsDC = updatedDocsDC[0];
                        }
                        return callback(null, numReplaced, updatedDocsDC);

                    });
                });
            }]);
    }

    /**@desc Updates all matching documents
    */
    update() {
        this.executor.push({ this: this, fn: this._update, arguments: arguments });
    }

    /**@desc Remove all docs matching the query
     * @private
     * @param {Object} query
     * @param {Boolean} [options.multi=false]
     * @param {Function} [cb] signature: err, numRemoved
     */
    _remove(query, options, callback = () => {}) {
        let self = this,
            numRemoved = 0,
            removedDocs = [],
            multi;

        if (typeof options === 'function') {
            callback = options;
            options = {};
        }
        multi = options.multi !== undefined ? options.multi : false;

        this.getCandidates(query, true, (err, candidates) => {
            if (err) {
                return callback(err);
            }

            try {
                candidates.forEach(d => {
                    if (model.match(d, query) && (multi || numRemoved === 0)) {
                        numRemoved += 1;
                        removedDocs.push({ $$deleted: true, _id: d._id });
                        self.removeFromIndexes(d);
                    }
                });
            } catch (err) {
                return callback(err);
            }

            self.persistence.persistNewState(removedDocs, err => {
                if (err) {
                    return callback(err);
                }
                return callback(null, numRemoved);
            });
        });
    }

    /**@desc Removes all matching documents
    */
    remove() {
        this.executor.push({ this: this, fn: this._remove, arguments: arguments });
    }
};
