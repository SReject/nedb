// node modules
const util = require('util');

// dep modules
const _ = require('underscore');

// lib modules
const helpers = require('./customUtils');

// begin
const checkKey = helpers.checkKey;
const checkObject = helpers.checkObject;
const getDotValue = helpers.getDotValue;
const areThingsEqual = helpers.areThingsEqual;
const isPrimitiveType = helpers.isPrimitiveType;
const deepCopy = helpers.deepCopy;
const areComparable = helpers.areComparable;
const compareThings = helpers.compareThings;

let modifierFunctions = {},
    matchQueryPart,
    match,
    modify,
    modifierFactory = (modifier, fn) => {
        modifierFunctions[modifier] = function (obj, field, value) {
            let fieldParts = typeof field === 'string' ? field.split('.') : field;

            if (fieldParts.length === 1) {
                lastStepModifierFunctions[modifier](obj, field, value); //eslint-disable-line no-use-before-define

            } else {
                if (obj[fieldParts[0]] === undefined) {
                    if (modifier === '$unset') {
                        return;
                    } // Bad looking specific fix, needs to be generalized modifiers that behave like $unset are implemented
                    obj[fieldParts[0]] = {};
                }
                modifierFunctions[modifier](obj[fieldParts[0]], fieldParts.slice(1), value);
            }
        };

        return fn;
    };

/**@desc The signature of modifier functions is as follows
 * Their structure is always the same: recursively follow the dot notation while creating
 * the nested documents if needed, then apply the "last step modifier"
 * @param {Object} obj The model to modify
 * @param {String} field Can contain dots, in that case that means we will set a subfield recursively
 * @param {Model} value
 */
const lastStepModifierFunctions = {
    $set: modifierFactory('$set', (obj, field, value) => {
        obj[field] = value;
    }),

    $unset: modifierFactory('$unset', (obj, field) => {
        delete obj[field];
    }),

    $push: modifierFactory('$push', (obj, field, value) => {
        // Create the array if it doesn't exist
        if (!obj.hasOwnProperty(field)) {
            obj[field] = [];
        }

        if (!Array.isArray(obj[field])) {
            throw new Error("Can't $push an element on non-array values");
        }

        if (value !== null && typeof value === 'object' && value.$slice && value.$each === undefined) {
            value.$each = [];
        }

        if (value !== null && typeof value === 'object' && value.$each) {
            if (Object.keys(value).length >= 3 || (Object.keys(value).length === 2 && value.$slice === undefined)) {
                throw new Error("Can only use $slice in cunjunction with $each when $push to array");
            }
            if (!Array.isArray(value.$each)) {
                throw new Error("$each requires an array value");
            }

            value.$each.forEach(function (v) {
                obj[field].push(v);
            });

            if (value.$slice === undefined || typeof value.$slice !== 'number') {
                return;
            }

            if (value.$slice === 0) {
                obj[field] = [];
            } else {
                let start, end, n = obj[field].length;
                if (value.$slice < 0) {
                    start = Math.max(0, n + value.$slice);
                    end = n;
                } else if (value.$slice > 0) {
                    start = 0;
                    end = Math.min(n, value.$slice);
                }
                obj[field] = obj[field].slice(start, end);
            }
        } else {
            obj[field].push(value);
        }
    }),

    $addToSet: modifierFactory('$addToSet', (obj, field, value) => {
        let addToSet = true;

        // Create the array if it doesn't exist
        if (!obj.hasOwnProperty(field)) {
            obj[field] = [];
        }

        if (!Array.isArray(obj[field])) {
            throw new Error("Can't $addToSet an element on non-array values");
        }

        if (value !== null && typeof value === 'object' && value.$each) {
            if (Object.keys(value).length > 1) {
                throw new Error("Can't use another field in conjunction with $each");
            }
            if (!Array.isArray(value.$each)) {
                throw new Error("$each requires an array value");
            }

            value.$each.forEach(v => lastStepModifierFunctions.$addToSet(obj, field, v));
        } else {
            obj[field].forEach(function (v) {
                if (compareThings(v, value) === 0) {
                    addToSet = false;
                }
            });
            if (addToSet) {
                obj[field].push(value);
            }
        }
    }),

    $pop: modifierFactory('$pop', (obj, field, value) => {
        if (!Array.isArray(obj[field])) {
            throw new Error("Can't $pop an element from non-array values");
        }
        if (typeof value !== 'number') {
            throw new Error(value + " isn't an integer, can't use it with $pop");
        }
        if (value === 0) {
            return;
        }

        if (value > 0) {
            obj[field] = obj[field].slice(0, obj[field].length - 1);
        } else {
            obj[field] = obj[field].slice(1);
        }
    }),

    $pull: modifierFactory('$pull', (obj, field, value) => {
        let arr, i;

        if (!Array.isArray(obj[field])) {
            throw new Error("Can't $pull an element from non-array values");
        }

        arr = obj[field];
        for (i = arr.length - 1; i >= 0; i -= 1) {
            if (match(arr[i], value)) {
                arr.splice(i, 1);
            }
        }
    }),

    $inc: modifierFactory('$inc', (obj, field, value) => {
        if (typeof value !== 'number') {
            throw new Error(value + " must be a number");
        }

        if (typeof obj[field] !== 'number') {
            if (!_.has(obj, field)) {
                obj[field] = value;
            } else {
                throw new Error("Don't use the $inc modifier on non-number fields");
            }
        } else {
            obj[field] += value;
        }
    }),

    $min: modifierFactory('$min', (obj, field, value) => {
        if (typeof obj[field] === 'undefined') {
            obj[field] = value;
        } else if (value < obj[field]) {
            obj[field] = value;
        }
    }),

    $max: modifierFactory('$max', (obj, field, value) => {
        if (typeof obj[field] === 'undefined') {
            obj[field] = value;
        } else if (value > obj[field]) {
            obj[field] = value;
        }
    })
};

const arrayComparisonFunctions = {
    $size: true,
    $elemMatch: true
};

const logicalOperators = {
    /**
     * Inverted match of the query
     * @param {Model} obj
     * @param {Query} query
     */
    $not: (obj, query) => !match(obj, query),

    /**
     * Use a function to match
     * @param {Model} obj
     * @param {Query} query
     */
    $where: (obj, fn) => {
        let result;

        if (!_.isFunction(fn)) {
            throw new Error("$where operator used without a function");
        }

        result = fn.call(obj);
        if (!_.isBoolean(result)) {
            throw new Error("$where function must return boolean");
        }

        return result;
    },

    /**
     * Match any of the subqueries
     * @param {Model} obj
     * @param {Array of Queries} query
     */
    $or: (obj, query) => {
        let i;

        if (!Array.isArray(query)) {
            throw new Error("$or operator used without an array");
        }

        for (i = 0; i < query.length; i += 1) {
            if (match(obj, query[i])) {
                return true;
            }
        }

        return false;
    },

    /**
     * Match all of the subqueries
     * @param {Model} obj
     * @param {Array of Queries} query
     */
    $and: (obj, query) => {
        let i;
        if (!Array.isArray(query)) {
            throw new Error("$and operator used without an array");
        }
        for (i = 0; i < query.length; i += 1) {
            if (!match(obj, query[i])) {
                return false;
            }
        }
        return true;
    }
};

const comparisonFunctions = {

    /**@desc Arithmetic and comparison operators
     * @param {Native value} a Value in the object
     * @param {Native value} b Value in the query
     */
    $lt: (a, b) => areComparable(a, b) && a < b,
    $lte: (a, b) => areComparable(a, b) && a <= b,

    $gt: (a, b) => areComparable(a, b) && a > b,
    $gte: (a, b) => areComparable(a, b) && a >= b,

    $ne: (a, b) => {
        return a === undefined ? true : !areThingsEqual(a, b);
    },

    $in: (a, b) => {
        let i;

        if (!Array.isArray(b)) {
            throw new Error("$in operator called with a non-array");
        }

        for (i = 0; i < b.length; i += 1) {
            if (areThingsEqual(a, b[i])) {
                return true;
            }
        }

        return false;
    },

    $nin: (a, b) => {
        if (!Array.isArray(b)) {
            throw new Error("$nin operator called with a non-array");
        }

        return !comparisonFunctions.$in(a, b);
    },

    $regex: (a, b) => {
        if (!util.isRegExp(b)) {
            throw new Error("$regex operator called with non regular expression");
        }

        if (typeof a !== 'string') {
            return false;
        }
        return b.test(a);
    },

    $exists: (value, exists) => {
        if (exists || exists === '') { // This will be true for all values of exists except false, null, undefined and 0
            exists = true; // That's strange behaviour (we should only use true/false) but that's the way Mongo does it...
        } else {
            exists = false;
        }

        if (value === undefined) {
            return !exists;
        }
        return exists;
    },

    // specific to arrays
    $size: (obj, value) => {
        if (!Array.isArray(obj)) {
            return false;
        }
        if (value % 1 !== 0) {
            throw new Error("$size operator called without an integer");
        }

        return (obj.length === value);
    },

    $elemMatch: (obj, value) => {
        if (!Array.isArray(obj)) {
            return false;
        }
        let i = obj.length;
        let result = false; // Initialize result
        while (i--) {
            if (match(obj[i], value)) { // If match for array element, return true
                result = true;
                break;
            }
        }
        return result;
    }
};

/**
 * Match an object against a specific { key: value } part of a query
 * if the treatObjAsValue flag is set, don't try to match every part separately, but the array as a whole
 */
matchQueryPart = (obj, queryKey, queryValue, treatObjAsValue) => {
    let objValue = getDotValue(obj, queryKey),
        i,
        keys,
        firstChars,
        dollarFirstChars;

    // Check if the value is an array if we don't force a treatment as value
    if (Array.isArray(objValue) && !treatObjAsValue) {

        // If the queryValue is an array, try to perform an exact match
        if (Array.isArray(queryValue)) {
            return matchQueryPart(obj, queryKey, queryValue, true);
        }

        // Check if we are using an array-specific comparison function
        if (queryValue !== null && typeof queryValue === 'object' && !util.isRegExp(queryValue)) {
            keys = Object.keys(queryValue);
            for (i = 0; i < keys.length; i += 1) {
                if (arrayComparisonFunctions[keys[i]]) {
                    return matchQueryPart(obj, queryKey, queryValue, true);
                }
            }
        }

        // If not, treat it as an array of { obj, query } where there needs to be at least one match
        for (i = 0; i < objValue.length; i += 1) {
            if (matchQueryPart({ k: objValue[i] }, 'k', queryValue)) {
                return true;
            } // k here could be any string
        }
        return false;
    }

    // queryValue is an actual object. Determine whether it contains comparison operators
    // or only normal fields. Mixed objects are not allowed
    if (
        queryValue !== null &&
        typeof queryValue === 'object' &&
        !util.isRegExp(queryValue) &&
        !Array.isArray(queryValue)
    ) {
        keys = Object.keys(queryValue);
        firstChars = keys.map(item => item[0]);
        dollarFirstChars = firstChars.filter(item => item === '$');

        if (dollarFirstChars.length !== 0 && dollarFirstChars.length !== firstChars.length) {
            throw new Error("You cannot mix operators and normal fields");
        }

        // queryValue is an object of this form: { $comparisonOperator1: value1, ... }
        if (dollarFirstChars.length > 0) {
            for (i = 0; i < keys.length; i += 1) {
                if (!comparisonFunctions[keys[i]]) {
                    throw new Error("Unknown comparison function " + keys[i]);
                }

                if (!comparisonFunctions[keys[i]](objValue, queryValue[keys[i]])) {
                    return false;
                }
            }
            return true;
        }
    }

    // Using regular expressions with basic querying
    if (util.isRegExp(queryValue)) {
        return comparisonFunctions.$regex(objValue, queryValue);
    }

    // queryValue is either a native value or a normal object
    // Basic matching is possible
    if (!areThingsEqual(objValue, queryValue)) {
        return false;
    }

    return true;
};


/**
 * Tell if a given document matches a query
 * @param {Object} obj Document to check
 * @param {Object} query
 */
match = (obj, query) => {
    let queryKeys,
        queryKey,
        queryValue,
        i;

    // Primitive query against a primitive type
    // This is a bit of a hack since we construct an object with an arbitrary key only to dereference it later
    // But I don't have time for a cleaner implementation now
    if (isPrimitiveType(obj) || isPrimitiveType(query)) {
        return matchQueryPart({ needAKey: obj }, 'needAKey', query);
    }

    // Normal query
    queryKeys = Object.keys(query);
    for (i = 0; i < queryKeys.length; i += 1) {
        queryKey = queryKeys[i];
        queryValue = query[queryKey];

        if (queryKey[0] === '$') {
            if (!logicalOperators[queryKey]) {
                throw new Error("Unknown logical operator " + queryKey);
            }
            if (!logicalOperators[queryKey](obj, queryValue)) {
                return false;
            }
        } else {
            if (!matchQueryPart(obj, queryKey, queryValue)) {
                return false;
            }
        }
    }
    return true;
};


/**@desc Modify a DB object according to an update query
 */
modify = (obj, updateQuery) => {
    let keys = Object.keys(updateQuery),
        firstChars = _.map(keys, function (item) {
            return item[0];
        }),
        dollarFirstChars = _.filter(firstChars, function (c) {
            return c === '$';
        }),
        newDoc, modifiers;
    if (keys.indexOf('_id') !== -1 && updateQuery._id !== obj._id) {
        throw new Error("You cannot change a document's _id");
    }

    if (dollarFirstChars.length !== 0 && dollarFirstChars.length !== firstChars.length) {
        throw new Error("You cannot mix modifiers and normal fields");
    }

    if (dollarFirstChars.length === 0) {
    // Simply replace the object with the update query contents
        newDoc = deepCopy(updateQuery);
        newDoc._id = obj._id;
    } else {
        // Apply modifiers
        modifiers = _.uniq(keys);
        newDoc = deepCopy(obj);
        modifiers.forEach(function (m) {
            let keys;

            if (!modifierFunctions[m]) {
                throw new Error("Unknown modifier " + m);
            }

            // Can't rely on Object.keys throwing on non objects since ES6
            // Not 100% satisfying as non objects can be interpreted as objects but no false negatives so we can live with it
            if (typeof updateQuery[m] !== 'object') {
                throw new Error("Modifier " + m + "'s argument must be an object");
            }

            keys = Object.keys(updateQuery[m]);
            keys.forEach(function (k) {
                modifierFunctions[m](newDoc, k, updateQuery[m][k]);
            });
        });
    }

    // Check result is valid and return it
    checkObject(newDoc);

    if (obj._id !== newDoc._id) {
        throw new Error("You can't change a document's _id");
    }
    return newDoc;
};


// Interface
module.exports.deepCopy = deepCopy;
module.exports.checkObject = checkObject;
module.exports.isPrimitiveType = isPrimitiveType;
module.exports.modify = modify;
module.exports.getDotValue = getDotValue;
module.exports.match = match;
module.exports.areThingsEqual = areThingsEqual;
module.exports.compareThings = compareThings;


/**@desc Serialize an object to be persisted to a one-line string
 */
// cannot use lambda as the replacer function as 'this' needs to be perserved
module.exports.serialize = obj => JSON.stringify(obj, function(key, value) {
    checkKey(key, value);

    value = this[key];

    if (value === undefined) {
        return undefined;
    }
    if (value === null) {
        return null;
    }

    // check if input is date
    if (value instanceof Date && typeof value.getTime === 'function') {
        return { $$date: this[key].getTime() };
    }

    return value;
});

/**@desc deserializes specified raw data
 */
module.exports.deserialize = rawData => JSON.parse(rawData, function (key, value) {
    if (key === '$$date') {
        return new Date(value);
    }
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean' || value === null) {
        return value;
    }
    if (value && value.$$date) {
        return value.$$date;
    }

    return value;
});
