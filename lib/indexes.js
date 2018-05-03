let BinarySearchTree = require('binary-search-tree').AVLTree,
    model = require('./model'),
    _ = require('underscore'),
    util = require('util');

/**
 * Two indexed pointers are equal iif they point to the same place
 */
function checkValueEquality (a, b) {
    return a === b;
}

/**
 * Type-aware projection
 */
function projectForUnique (elt) {
    if (elt === null) {
        return '$null';
    }
    if (typeof elt === 'string') {
        return '$string' + elt;
    }
    if (typeof elt === 'boolean') {
        return '$boolean' + elt;
    }
    if (typeof elt === 'number') {
        return '$number' + elt;
    }
    if (util.isArray(elt)) {
        return '$date' + elt.getTime();
    }

    return elt; // Arrays and objects, will check for pointer equality
}

module.exports = class Index {

    /**@desc Create a new index
     * @constructor
     * @param {String} options.fieldName On which field should the index apply (can use dot notation to index on sub fields)
     * @param {Boolean} options.unique Optional, enforce a unique constraint (default: false)
     * @param {Boolean} options.sparse Optional, allow a sparse index (we can have documents for which fieldName is undefined) (default: false)
     */
    constructor(options) {
        this.fieldName = options.fieldName;
        this.unique = options.unique || false;
        this.sparse = options.sparse || false;
        this.treeOptions = { unique: this.unique, compareKeys: model.compareThings, checkValueEquality: checkValueEquality };
        this.reset();
    }

    /**@desc Reset an index
     * @param {Object|Array} [newData] - Data to initialize the index with
     */
    reset(newData) {
        this.tree = new BinarySearchTree(this.treeOptions);

        if (newData) {
            this.insert(newData);
        }
    }

    /**@desc Insert a new document in the index
     * @param {Object|Array} doc - Doc/s to insert
     */
    insert(doc) {
        let key,
            keys,
            i,
            failingI,
            error;
        if (util.isArray(doc)) {
            this.insertMultipleDocs(doc); return;
        }

        key = model.getDotValue(doc, this.fieldName);

        // We don't index documents that don't contain the field if the index is sparse
        if (key === undefined && this.sparse) {
            return;
        }

        if (!util.isArray(key)) {
            this.tree.insert(key, doc);
        } else {
        // If an insert fails due to a unique constraint, roll back all inserts before it
            keys = _.uniq(key, projectForUnique);

            for (i = 0; i < keys.length; i += 1) {
                try {
                    this.tree.insert(keys[i], doc);
                } catch (e) {
                    error = e;
                    failingI = i;
                    break;
                }
            }

            if (error) {
                for (i = 0; i < failingI; i += 1) {
                    this.tree.delete(keys[i], doc);
                }

                throw error;
            }
        }
    }

    /**@desc Insert an array of documents in the index
     * @param {Array} docs
     * @private
     */
    insertMultipleDocs(docs) {
        let i, error, failingI;

        for (i = 0; i < docs.length; i += 1) {
            try {
                this.insert(docs[i]);
            } catch (e) {
                error = e;
                failingI = i;
                break;
            }
        }

        if (error) {
            for (i = 0; i < failingI; i += 1) {
                this.remove(docs[i]);
            }

            throw error;
        }
    }

    /**@desc Remove a document from the index
     * @param {Object} doc
     */
    remove(doc) {
        let key, self = this;

        if (util.isArray(doc)) {
            doc.forEach(function (d) {
                self.remove(d);
            }); return;
        }

        key = model.getDotValue(doc, this.fieldName);

        if (key === undefined && this.sparse) {
            return;
        }

        if (!util.isArray(key)) {
            this.tree.delete(key, doc);
        } else {
            _.uniq(key, projectForUnique).forEach(function (_key) {
                self.tree.delete(_key, doc);
            });
        }
    }

    /**@desc Update a document in the index
     * @param {Object} oldDoc
     * @param {Object} newDoc
     */
    update(oldDoc, newDoc) {
        if (util.isArray(oldDoc)) {
            this.updateMultipleDocs(oldDoc); return;
        }

        this.remove(oldDoc);

        try {
            this.insert(newDoc);
        } catch (e) {
            this.insert(oldDoc);
            throw e;
        }
    }

    /**@desc Update multiple documents in the index
     * @param {Array} pairs
     * @private
     */
    updateMultipleDocs(pairs) {
        let i, failingI, error;

        for (i = 0; i < pairs.length; i += 1) {
            this.remove(pairs[i].oldDoc);
        }

        for (i = 0; i < pairs.length; i += 1) {
            try {
                this.insert(pairs[i].newDoc);
            } catch (e) {
                error = e;
                failingI = i;
                break;
            }
        }

        // If an error was raised, roll back changes in the inverse order
        if (error) {
            for (i = 0; i < failingI; i += 1) {
                this.remove(pairs[i].newDoc);
            }

            for (i = 0; i < pairs.length; i += 1) {
                this.insert(pairs[i].oldDoc);
            }

            throw error;
        }
    }

    /**@desc Revert an update
     * @param {Object} oldDoc
     * @param {Object} newDoc
     */
    revertUpdate(oldDoc, newDoc) {
        let revert = [];

        if (!util.isArray(oldDoc)) {
            this.update(newDoc, oldDoc);
        } else {
            oldDoc.forEach(function (pair) {
                revert.push({ oldDoc: pair.newDoc, newDoc: pair.oldDoc });
            });
            this.update(revert);
        }
    }

    /**@desc Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
     * @param {Object|Array} value Value to match the key against
     * @return {Array<Documents>}
     */
    getMatching(value) {
        let self = this;

        if (!util.isArray(value)) {
            return self.tree.search(value);
        }
        let _res = {}, res = [];

        value.forEach(function (v) {
            self.getMatching(v).forEach(function (doc) {
                _res[doc._id] = doc;
            });
        });

        Object.keys(_res).forEach(function (_id) {
            res.push(_res[_id]);
        });

        return res;
    }

    /**@desc Get all documents in index whose key is between bounds are they are defined by query
     * @param {Object} query
     * @return {Array<Documents>}
     */
    getBetweenBounds(query) {
        return this.tree.betweenBounds(query);
    }

    /**@desc Get all elements in the index
     * @return {Array<Documents>}
     */
    getAll() {
        let res = [];

        this.tree.executeOnEveryNode(function (node) {
            let i;

            for (i = 0; i < node.data.length; i += 1) {
                res.push(node.data[i]);
            }
        });

        return res;
    }
};
