// native modules
const crypto = require('crypto');
const util = require('util');


/**@desc Return a random alphanumerical string of length len
 */
function uid(len) {
    return crypto.randomBytes(Math.ceil(Math.max(8, len * 2)))
        .toString('base64')
        .replace(/[+/]/g, '')
        .slice(0, len);
}
module.exports.uid = uid;

/**@desc Promise-fies functions that use the node-callback progma
 * @param {Function} fn - Function to Promisfy
 * @returns {Function}
 */
function promisfy(fn) {
    return function (...args) {
        return new Promise((resolve, reject) => {
            args.push((err, ...result) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(...result);
                }
            });
            fn(...args);
        });
    };
}
module.exports.promisfy = promisfy;

/**@desc Check a key, throw an error if the key is non valid
 * @param {String} key key
 * @param {Model} value value, needed to treat the Date edge case
 */
function checkKey(key, value) {
    if (typeof key === 'number') {
        key = key.toString();
    }

    if (
        key[0] === '$' &&
        !(key === '$$date' && typeof value === 'number') &&
        !(key === '$$deleted' && value === true) &&
        !(key === '$$indexCreated') &&
        !(key === '$$indexRemoved')
    ) {
        throw new Error('Field names cannot begin with the $ character');
    }

    if (key.indexOf('.') !== -1) {
        throw new Error('Field names cannot contain a .');
    }
}
module.exports.checkKey = checkKey;

/**@desc Check a DB object and throw an error if it's not valid
 * Works by applying the above checkKey function to all fields recursively
 */
function checkObject(obj) {
    if (Array.isArray(obj)) {
        obj.forEach(obj => checkObject(obj));

    } else if (typeof obj === 'object' && obj !== null) {
        Object.keys(obj).forEach(key => {
            checkKey(key, obj[key]);
            checkObject(obj[key]);
        });
    }
}
module.exports.checkObject = checkObject;

/**@desc Get a value from object with dot notation
 * @param {Object} obj
 * @param {String} field
 */
function getDotValue(obj, field) {
    let fieldParts = typeof field === 'string' ? field.split('.') : field,
        i,
        objs;

    // field cannot be empty so that means we should return undefined so that nothing can match
    if (!obj) {
        return undefined;
    }

    if (fieldParts.length === 0) {
        return obj;
    }

    if (fieldParts.length === 1) {
        return obj[fieldParts[0]];
    }


    if (Array.isArray(obj[fieldParts[0]])) {

        // If the next field is an integer, return only this item of the array
        i = parseInt(fieldParts[1], 10);
        if (typeof i === 'number' && !isNaN(i)) {
            return getDotValue(obj[fieldParts[0]][i], fieldParts.slice(2));
        }

        // Return the array of values
        objs = new Array();
        for (i = 0; i < obj[fieldParts[0]].length; i += 1) {
            objs.push(getDotValue(obj[fieldParts[0]][i], fieldParts.slice(1)));
        }
        return objs;
    }
    return getDotValue(obj[fieldParts[0]], fieldParts.slice(1));
}
module.exports.getDotValue = getDotValue;

/**@desc Check whether 'things' are equal
 */
function areThingsEqual(a, b) {
    let aKeys, bKeys, i;

    // Strings, booleans, numbers, null
    if (
        a === null ||
        typeof a === 'string' ||
        typeof a === 'boolean' ||
        typeof a === 'number' ||
        b === null ||
        typeof b === 'string' ||
        typeof b === 'boolean' ||
        typeof b === 'number'
    ) {
        return a === b;
    }

    // Dates
    if (util.isDate(a) || util.isDate(b)) {
        return util.isDate(a) && util.isDate(b) && a.getTime() === b.getTime();
    }

    // Arrays (no match since arrays are used as a $in)
    // undefined (no match since they mean field doesn't exist and can't be serialized)
    if (
        a === undefined ||
        b === undefined ||
        Array.isArray(a) !== Array.isArray(b)
    ) {
        return false;
    }

    // General objects (check for deep equality)
    // a and b should be objects at this point
    try {
        aKeys = Object.keys(a);
        bKeys = Object.keys(b);
    } catch (e) {
        return false;
    }

    if (aKeys.length !== bKeys.length) {
        return false;
    }
    for (i = 0; i < aKeys.length; i += 1) {
        if (bKeys.indexOf(aKeys[i]) === -1) {
            return false;
        }
        if (!areThingsEqual(a[aKeys[i]], b[aKeys[i]])) {
            return false;
        }
    }
    return true;
}
module.exports.areThingsEqual = areThingsEqual;

/**@desc Tells if an object is a primitive type or a "real" object
 */
function isPrimitiveType (obj) {
    return typeof obj === 'boolean' ||
        typeof obj === 'number' ||
        typeof obj === 'string' ||
        obj === null ||
        util.isDate(obj) ||
        Array.isArray(obj);
}
module.exports.isPrimitiveType = isPrimitiveType;

/**@desc Deep copy a DB object
 * The optional strictKeys flag (defaulting to false) indicates whether to copy everything or only fields
 * where the keys are valid, i.e. don't begin with $ and don't contain a .
 */
function deepCopy(obj, strictKeys) {
    let res;

    if (
        typeof obj === 'boolean' ||
        typeof obj === 'number' ||
        typeof obj === 'string' ||
        obj === null ||
        util.isDate(obj)
    ) {
        return obj;
    }

    if (Array.isArray(obj)) {
        res = [];
        obj.forEach(o => res.push(deepCopy(o, strictKeys)));
        return res;
    }

    if (typeof obj === 'object') {
        res = {};
        Object.keys(obj).forEach(key => {
            if (!strictKeys || (key[0] !== '$' && key.indexOf('.') === -1)) {
                res[key] = deepCopy(obj[key], strictKeys);
            }
        });
        return res;
    }

    // For now everything else is undefined. We should probably throw an error instead
    return undefined;
}
module.exports.deepCopy = deepCopy;

/**@desc Check that two values are comparable
 */
function areComparable(a, b) {
    if (
        typeof a !== 'string' && typeof a !== 'number' && !util.isDate(a) &&
        typeof b !== 'string' && typeof b !== 'number' && !util.isDate(b)
    ) {
        return false;
    }

    if (typeof a !== typeof b) {
        return false;
    }

    return true;
}
module.exports.areComparable = areComparable;

/**@desc Utility functions for comparing things
 */
function compareNSB(a, b) {
    if (a < b) {
        return -1;
    }
    if (a > b) {
        return 1;
    }
    return 0;
}
module.exports.compareNSB = compareNSB;

/**@desc Compares items between two arrays for equalness
 */
function compareArrays(a, b) {
    let i, comp;

    for (i = 0; i < Math.min(a.length, b.length); i += 1) {
        comp = compareThings(a[i], b[i]); //eslint-disable-line no-use-before-define

        if (comp !== 0) {
            return comp;
        }
    }

    // Common section was identical, longest one wins
    return compareNSB(a.length, b.length);
}
module.exports.compareArrays = compareArrays;

/**@desc Compare two instances
*/
function compareThings(a, b, _compareStrings) {
    let aKeys,
        bKeys,
        comp,
        i,
        compareStrings = _compareStrings || compareNSB;

    // undefined
    if (a === undefined) {
        return b === undefined ? 0 : -1;
    }
    if (b === undefined) {
        return a === undefined ? 0 : 1;
    }

    // null
    if (a === null) {
        return b === null ? 0 : -1;
    }
    if (b === null) {
        return a === null ? 0 : 1;
    }

    // Numbers
    if (typeof a === 'number') {
        return typeof b === 'number' ? compareNSB(a, b) : -1;
    }
    if (typeof b === 'number') {
        return typeof a === 'number' ? compareNSB(a, b) : 1;
    }

    // Strings
    if (typeof a === 'string') {
        return typeof b === 'string' ? compareStrings(a, b) : -1;
    }
    if (typeof b === 'string') {
        return typeof a === 'string' ? compareStrings(a, b) : 1;
    }

    // Booleans
    if (typeof a === 'boolean') {
        return typeof b === 'boolean' ? compareNSB(a, b) : -1;
    }
    if (typeof b === 'boolean') {
        return typeof a === 'boolean' ? compareNSB(a, b) : 1;
    }

    // Dates
    if (util.isDate(a)) {
        return util.isDate(b) ? compareNSB(a.getTime(), b.getTime()) : -1;
    }
    if (util.isDate(b)) {
        return util.isDate(a) ? compareNSB(a.getTime(), b.getTime()) : 1;
    }

    // Arrays (first element is most significant and so on)
    if (Array.isArray(a)) {
        return Array.isArray(b) ? compareArrays(a, b) : -1;
    }
    if (Array.isArray(b)) {
        return Array.isArray(a) ? compareArrays(a, b) : 1;
    }

    // Objects
    aKeys = Object.keys(a).sort();
    bKeys = Object.keys(b).sort();

    for (i = 0; i < Math.min(aKeys.length, bKeys.length); i += 1) {
        comp = compareThings(a[aKeys[i]], b[bKeys[i]]);

        if (comp !== 0) {
            return comp;
        }
    }

    return compareNSB(aKeys.length, bKeys.length);
}
module.exports.compareThings = compareThings;
