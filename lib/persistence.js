const PATH = require('path');
const storage = require('./storage');
const model = require('./model');
const async = require('async');
const customUtils = require('./customUtils');
const Index = require('./indexes');

module.exports = class Persistence {

    /**@desc Create a new Persistence object for database options.db
     * @param {Datastore} options.db
     * @param {Boolean} options.nodeWebkitAppName Optional, specify the name of your NW app if you want options.filename to be relative to the directory where
     *                                            Node Webkit stores application data such as cookies and local storage (the best place to store data in my opinion)
     */
    constructor(options) {
        let i, j, randomString;

        this.db = options.db;
        this.inMemoryOnly = this.db.inMemoryOnly;
        this.filename = this.db.filename;
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

        // For NW apps, store data in the same directory where NW stores application data
        if (this.filename && options.nodeWebkitAppName) {
            console.log("==================================================================");
            console.log("WARNING: The nodeWebkitAppName option is deprecated");
            console.log("To get the path to the directory where Node Webkit stores the data");
            console.log("for your app, use the internal nw.gui module like this");
            console.log("require('nw.gui').App.dataPath");
            console.log("See https://github.com/rogerwang/node-webkit/issues/500");
            console.log("==================================================================");
            this.filename = Persistence.getNWAppFilename(options.nodeWebkitAppName, this.filename);
        }
    }

    /**@desc Servers as a compaction function
     * @param {Function} cb
     */
    persistCachedDatabase(callback = () => {}) {
        let toPersist = '',
            self = this;

        if (this.inMemoryOnly) {
            return callback(null);
        }

        this.db.getAllData().forEach(function (doc) {
            toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
        });
        Object.keys(this.db.indexes).forEach(function (fieldName) {
            if (fieldName !== "_id") { // The special _id index is managed by datastore.js, the others need to be persisted
                toPersist += self.afterSerialization(model.serialize({ $$indexCreated: { fieldName: fieldName, unique: self.db.indexes[fieldName].unique, sparse: self.db.indexes[fieldName].sparse }})) + '\n';
            }
        });

        storage.crashSafeWriteFile(this.filename, toPersist, function (err) {
            if (err) {
                return callback(err);
            }
            self.db.emit('compaction.done');
            callback(null);
        });
    }

    /**@desc Queue a rewrite of the datafile
     */
    compactDatafile() {
        this.db.executor.push({ this: this, fn: this.persistCachedDatabase, arguments: [] });
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
    persistNewState(newDocs, callback = () => {}) {
        let self = this,
            toPersist = '';

        // In-memory only datastore
        if (self.inMemoryOnly) {
            return callback(null);
        }

        newDocs.forEach(function (doc) {
            toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
        });

        if (toPersist.length === 0) {
            return callback(null);
        }

        storage.appendFile(self.filename, toPersist, 'utf8', callback);
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
     * @param {Function} [cb] signature: err
     */
    loadDatabase(callback = () => {}) {
        let self = this;
        self.db.resetIndexes();

        // In-memory only datastore
        if (self.inMemoryOnly) {
            return callback(null);
        }

        async.waterfall([
            cb => {
                Persistence.ensureDirectoryExists(PATH.dirname(self.filename), () => {
                    storage.ensureDatafileIntegrity(self.filename, () => {
                        storage.readFile(self.filename, 'utf8', (err, rawData) => {
                            let treatedData;
                            if (err) {
                                return cb(err);
                            }

                            try {
                                treatedData = self.treatRawData(rawData);
                            } catch (e) {
                                return cb(e);
                            }

                            // Recreate all indexes in the datafile
                            Object.keys(treatedData.indexes).forEach(function (key) {
                                self.db.indexes[key] = new Index(treatedData.indexes[key]);
                            });

                            // Fill cached database (i.e. all indexes) with data
                            try {
                                self.db.resetIndexes(treatedData.data);
                            } catch (e) {
                                self.db.resetIndexes(); // Rollback any index which didn't fail
                                return cb(e);
                            }

                            self.db.persistence.persistCachedDatabase(cb);
                        });
                    });
                });
            }
        ], function (err) {
            if (err) {
                return callback(err);
            }

            self.db.executor.processBuffer();
            callback(null);
        });
    }


    /**@desc Check if directory exists; create it if it does not
     * @param {String} dir
     * @param {Function} cb
     */
    static ensureDirectoryExists(dir, cb) {
        storage.mkdirp(dir, cb || (() => {}));
    }

    /**@desc Returns relative path to Node Webkit storage
     * @param {String} appName
     * @param {String} filename
     * @returns {String}
     */
    static getNWAppFilename(appName, filename) {
        let home;
        switch (process.platform) {
        case 'win32':
        case 'win64':
            home = process.env.LOCALAPPDATA || process.env.APPDATA;
            if (!home) {
                throw new Error("Couldn't find the base application data folder");
            }
            home = PATH.join(home, appName);
            break;
        case 'darwin':
            home = process.env.HOME;
            if (!home) {
                throw new Error("Couldn't find the base application data directory");
            }
            home = PATH.join(home, 'Library', 'Application Support', appName);
            break;
        case 'linux':
            home = process.env.HOME;
            if (!home) {
                throw new Error("Couldn't find the base application data directory");
            }
            home = PATH.join(home, '.config', appName);
            break;
        default:
            throw new Error("Can't use the Node Webkit relative path for platform " + process.platform);
        }
        return PATH.join(home, 'nedb-data', filename);
    }
};
