// node modules
const path = require('path');

// lib modules
const storage = require('./storage');
const model = require('./model');
const customUtils = require('./customUtils');
const Index = require('./indexes');


module.exports = class Persistence {

    /**@desc Create a new Persistence object for database options.db
     * @constructor
     * @param {Datastore} options.db
     * @param {Number} options.corruptAlertThreshold
     * @param {Function} options.afterSerialization
     * @param {Function} options.beforeDeserialization
     */
    constructor(options) {

        let i,
            j,
            randomString,
            db = options.db;

        this.db = db;
        this.inMemoryOnly = db.inMemoryOnly;
        this.filename = db.filename;
        this.corruptAlertThreshold = options.corruptAlertThreshold !== undefined ? options.corruptAlertThreshold : 0.1;

        if (!this.inMemoryOnly && this.filename && this.filename.charAt(this.filename.length - 1) === '~') {
            throw new Error("The datafile name can't end with a ~, which is reserved for crash safe backup files");
        }

        // After serialization and before deserialization hooks with some basic sanity checks
        if (options.afterSerialization && !options.beforeDeserialization) {
            throw new Error("Serialization hook defined but deserialization hook undefined, cautiously refusing to start NeDB to prevent dataloss");
        }

        if (!options.afterSerialization && options.beforeDeserialization) {
            throw new Error("Serialization hook undefined but deserialization hook defined, cautiously refusing to start NeDB to prevent dataloss");
        }

        this.afterSerialization = options.afterSerialization || (s => s);
        this.beforeDeserialization = options.beforeDeserialization || (s => s);
        for (i = 1; i < 30; i += 1) {
            for (j = 0; j < 10; j += 1) {
                randomString = customUtils.uid(i);
                if (this.beforeDeserialization(this.afterSerialization(randomString)) !== randomString) {
                    throw new Error("beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NeDB to prevent dataloss");
                }
            }
        }
    }

    persistCachedDatabase() {
        let toPersist = '',
            self = this;

        if (this.inMemoryOnly) {
            return Promise.resolve();

        }
        this.db.getAllData().forEach(function (doc) {
            toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
        });
        Object.keys(this.db.indexes).forEach(function (fieldName) {
            if (fieldName !== "_id") { // The special _id index is managed by datastore.js, the others need to be persisted
                toPersist += self.afterSerialization(model.serialize({
                    $$indexCreated: {
                        fieldName: fieldName,
                        unique: self.db.indexes[fieldName].unique,
                        sparse: self.db.indexes[fieldName].sparse
                    }
                })) + '\n';
            }
        });

        return storage
            .crashSafeWriteFile(self.filename, toPersist)
            .then(() => {
                self.db.emit('compaction.done');
            });

    }

    /**@desc Queue a rewrite of the datafile
     */
    compactDatafile() {
        let self = this,
            fn = (cb = () => {}) => {
                self.persistCachedDatabase()
                    .then(() => {
                        cb(null);
                    }).catch(err => cb(err));
            };
        this.db.executor.push({ this: this, fn: fn, arguments: [] });
    }

    /**@desc Set automatic compaction every interval ms
     * @param {Number} interval in milliseconds, with an enforced minimum of 5 seconds
     */
    setAutocompactionInterval(interval) {
        let self = this,
            minInterval = 5000,
            realInterval = Math.max(interval || 0, minInterval);
        this.stopAutocompaction();
        this.autocompactionIntervalId = setInterval(() => self.compactDatafile(), realInterval);
    }

    /**@desc Stop automatic compaction
     */
    stopAutocompaction() {
        if (this.autocompactionIntervalId) {
            clearInterval(this.autocompactionIntervalId);
        }
    }

    /**@desc Persist new state for the given newDocs
     * @param {Array} newDocs Can be empty if no doc was updated/removed
     * @param {Function} [callback] Optional, signature: err
     */
    persistNewState(newDocs) {
        let self = this,
            toPersist = '';

        // In-memory only datastore
        if (self.inMemoryOnly) {
            return Promise.resolve();
        }

        newDocs.forEach(function (doc) {
            toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
        });

        if (toPersist.length === 0) {
            return Promise.resolve();
        }

        return storage.appendFile(self.filename, toPersist, 'utf8');
    }

    /**@desc Translates raw database data into a document/document-set
     * @param {rawData} string
     * @returns {Any}
     */
    treatRawData(rawData) {
        let data = rawData.split('\n'),
            dataById = {},
            tdata = [],
            i,
            indexes = {},
            corruptItems = -1; // Last line of every data file is usually blank so not really corrupt

        for (i = 0; i < data.length; i += 1) {
            try {
                let doc = model.deserialize(this.beforeDeserialization(data[i]));
                if (doc._id) {
                    if (doc.$$deleted === true) {
                        delete dataById[doc._id];
                    } else {
                        dataById[doc._id] = doc;
                    }
                } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName != null) {
                    indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
                } else if (typeof doc.$$indexRemoved === "string") {
                    delete indexes[doc.$$indexRemoved];
                }
            } catch (e) {
                corruptItems += 1;
            }
        }

        // A bit lenient on corruption
        if (data.length > 0 && corruptItems / data.length > this.corruptAlertThreshold) {
            throw new Error("More than " + Math.floor(100 * this.corruptAlertThreshold) + "% of the data file is corrupt, the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss");
        }

        Object.keys(dataById).forEach(function (k) {
            tdata.push(dataById[k]);
        });

        return { data: tdata, indexes: indexes };
    }

    /**@desc Loads the database
     * @returns {Promise<>}
     */
    loadDatabase() {
        let self = this;
        self.db.resetIndexes();

        // In-memory only datastore
        if (self.inMemoryOnly) {
            return Promise.resolve();
        }
        return storage
            .mkdirp(path.dirname(self.filename))
            .then(() => storage.ensureDatafileIntegrity(self.filename))
            .then(() => storage.readFile(self.filename, 'utf8'))
            .then(rawData => {
                let treatedData = self.treatRawData(rawData);

                // Recreate all indexes in the datafile
                Object.keys(treatedData.indexes).forEach((key) => {
                    self.db.indexes[key] = new Index(treatedData.indexes[key]);
                });

                // Fill cached database (i.e. all indexes) with data
                try {
                    self.db.resetIndexes(treatedData.data);

                } catch (err) {
                    self.db.resetIndexes(); // Rollback any index which didn't fail
                    return Promise.reject(err);
                }
                return self.db.persistence.persistCachedDatabase();
            })
            .then(() => {
                self.db.executor.processBuffer();
            });
    }

    /**@desc Check if directory exists; create it if it does not
     * @param {String} dir
     * @param {Function} cb
     */
    static ensureDirectoryExists(dir) {
        return storage.mkdirp(dir);
    }
};
