// native modules
const fs = require('fs');
const path = require('path');

// dep modules
const mkdirp = require('mkdirp');
const async = require('async');

// library modules
const promisfy = require('./customUtils').promisfy;

// exports
const storage = module.exports = {
    rename: promisfy(fs.rename),
    writeFile: promisfy(fs.writeFile),
    appendFile: promisfy(fs.appendFile),
    readFile: promisfy(fs.readFile),
    unlink: promisfy(fs.unlink),
    mkdirp: promisfy(mkdirp),

    exists: (path) => {
        return new Promise((resolve) => {
            fs.exists(path, (exists) => {
                resolve(exists);
            });
        });
    },

    /**@desc Deletes the specified file if it exists
     * @param {String} file - File to delete if it exists
     * @returns {Promise<>}
    */
    ensureFileDoesntExist: file => {
        return storage
            .exists(file)
            .then(exists => {
                if (exists) {
                    return storage.unlink(file);
                }
            });
    },

    /**@desc Flush data in OS buffer to storage if corresponding option is set
     * @param {String} options.filename
     * @param {Boolean} options.isDir Optional, defaults to false
     * @returns {Promise<>}
    */
    flushToStorage: options => {
        return new Promise((resolve, reject) => {
            let filename,
                flags;

            if (typeof options === 'string') {
                filename = options;
                flags = 'r+';
            } else {
                filename = options.filename;
                flags = options.isDir ? 'r' : 'r+';
            }

            if (flags === 'r' && (process.platform === 'win32' || process.platform === 'win64')) {
                resolve(null);

            } else {
                fs.open(filename, flags, (err, fd) => {
                    if (err) {
                        reject(err);

                    } else {
                        fs.fsync(fd, errFS => {
                            fs.close(fd, errC => {
                                if (errFS || errC) {
                                    err = new Error('Failed to flush to storage');
                                    err.errorOnFsync = errFS;
                                    err.errorOnClose = errC;
                                    reject(err);

                                } else {
                                    resolve(null);
                                }
                            });
                        });
                    }
                });
            }
        });
    },

    /**@desc Fully write or rewrite the datafile, immune to crashes during the write operation (data will not be lost)
     * @param {String} filename
     * @param {String} data
     * @returns {Promise<>}
    */
    crashSafeWriteFile: (filename, data) => {
        return new Promise((resolve, reject) => {
            let tempFilename = filename + '~';

            async.waterfall(
                [
                    cb => storage
                        .flushToStorage({filename: path.dirname(filename), isDir: true})
                        .then(() => cb(null))
                        .catch(cb),
                    cb => storage
                        .exists(filename)
                        .then(exists => {
                            if (exists) {
                                return storage.flushToStorage(filename);
                            }
                        })
                        .then(() => cb(null))
                        .catch(cb),
                    cb => storage
                        .writeFile(tempFilename, data)
                        .then(() => cb(null))
                        .catch(cb),
                    cb => storage
                        .flushToStorage(tempFilename)
                        .then(() => cb(null))
                        .catch(cb),
                    cb => storage
                        .rename(tempFilename, filename)
                        .then(() => cb(null))
                        .catch(cb),
                    cb => storage
                        .flushToStorage({filename: path.dirname(filename), isDir: true})
                        .then(() => cb(null))
                        .catch(cb)
                ],
                (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(null);
                    }
                });
        });
    },

    /**@desc Ensure the datafile contains all the data, even if there was a crash during a full file write
     * @param {String} filename
     * @returns {Promise<>}
     */
    ensureDatafileIntegrity: filename => {
        return storage
            .exists(filename)
            .then(filenameExists => {
                if (filenameExists) {
                    return;
                }
                let tempFilename = filename + '~';
                return storage
                    .exists(tempFilename)
                    .then(oldFilenameExists => {
                        if (oldFilenameExists) {
                            return storage.rename(tempFilename, filename);
                        }
                        // else
                        return storage.writeFile(filename, '', 'utf8');
                    });
            });
    }
};
