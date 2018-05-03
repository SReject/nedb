/**
 * Way data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage which chooses the best option depending on user browser (IndexedDB then WebSQL then localStorage)
 *
 * This version is the Node.js/Node Webkit version
 * It's essentially fs, mkdirp and crash safe write and read functions
 */

let fs = require('fs'),
    mkdirp = require('mkdirp'),
    async = require('async'),
    path = require('path'),
    storage = {};

storage.exists = fs.exists;
storage.rename = fs.rename;
storage.writeFile = fs.writeFile;
storage.unlink = fs.unlink;
storage.appendFile = fs.appendFile;
storage.readFile = fs.readFile;
storage.mkdirp = mkdirp;


/**
 * Explicit name ...
 */
storage.ensureFileDoesntExist = (file, callback) => {
    storage.exists(file, (exists) => {
        if (!exists) {
            callback(null);

        } else {
            storage.unlink(file, callback);
        }
    });
};


/**
 * Flush data in OS buffer to storage if corresponding option is set
 * @param {String} options.filename
 * @param {Boolean} options.isDir Optional, defaults to false
 * If options is a string, it is assumed that the flush of the file (not dir) called options was requested
 */
storage.flushToStorage = (options, callback) => {
    let filename,
        flags;

    if (typeof options === 'string') {
        filename = options;
        flags = 'r+';
    } else {
        filename = options.filename;
        flags = options.isDir ? 'r' : 'r+';
    }

    // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
    // except in the very rare event of the first time database is loaded and a crash happens
    if (flags === 'r' && (process.platform === 'win32' || process.platform === 'win64')) {
        return callback(null);
    }

    fs.open(filename, flags, (err, fd) => {
        if (err) {
            return callback(err);
        }
        fs.fsync(fd, errFS => {
            fs.close(fd, errC => {
                if (errFS || errC) {
                    let e = new Error('Failed to flush to storage');
                    e.errorOnFsync = errFS;
                    e.errorOnClose = errC;
                    callback(e);
                } else {
                    callback(null);
                }
            });
        });
    });
};


/**
 * Fully write or rewrite the datafile, immune to crashes during the write operation (data will not be lost)
 * @param {String} filename
 * @param {String} data
 * @param {Function} cb Optional callback, signature: err
 */
storage.crashSafeWriteFile = (filename, data, cb) => {
    let callback = cb || function () {},
        tempFilename = filename + '~';

    async.waterfall([
        async.apply(storage.flushToStorage, { filename: path.dirname(filename), isDir: true }),
        cb => storage.exists(filename, exists => {
            if (exists) {
                storage.flushToStorage(filename, cb);
            } else {
                cb();
            }
        }),
        cb => storage.writeFile(tempFilename, data, cb),
        async.apply(storage.flushToStorage, tempFilename),
        cb => storage.rename(tempFilename, filename, cb),
        async.apply(storage.flushToStorage, { filename: path.dirname(filename), isDir: true })
    ], callback);
};


/**
 * Ensure the datafile contains all the data, even if there was a crash during a full file write
 * @param {String} filename
 * @param {Function} callback signature: err
 */
storage.ensureDatafileIntegrity = (filename, callback) => {
    let tempFilename = filename + '~';

    storage.exists(filename, filenameExists => {

        // Write was successful
        if (filenameExists) {
            return callback(null);
        }

        storage.exists(tempFilename, function (oldFilenameExists) {

            // New database
            if (!oldFilenameExists) {
                return storage.writeFile(filename, '', 'utf8', callback);
            }

            // Write failed, use old version
            storage.rename(tempFilename, filename, callback);
        });
    });
};



// Interface
module.exports = storage;
