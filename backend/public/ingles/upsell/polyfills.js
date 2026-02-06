/**
 * Polyfills for IE11 compatibility
 */

// Array.prototype.forEach polyfill
if (!Array.prototype.forEach) {
    Array.prototype.forEach = function(callback, thisArg) {
        if (this == null) throw new TypeError('this is null or not defined');
        var O = Object(this);
        var len = O.length >>> 0;
        if (typeof callback !== 'function') throw new TypeError(callback + ' is not a function');
        var T = arguments.length > 1 ? thisArg : undefined;
        for (var k = 0; k < len; k++) {
            if (k in O) callback.call(T, O[k], k, O);
        }
    };
}

// NodeList.prototype.forEach polyfill
if (window.NodeList && !NodeList.prototype.forEach) {
    NodeList.prototype.forEach = Array.prototype.forEach;
}

// String.prototype.padStart polyfill
if (!String.prototype.padStart) {
    String.prototype.padStart = function padStart(targetLength, padString) {
        targetLength = targetLength >> 0;
        padString = String(typeof padString !== 'undefined' ? padString : ' ');
        if (this.length >= targetLength) {
            return String(this);
        }
        targetLength = targetLength - this.length;
        if (targetLength > padString.length) {
            padString += padString.repeat(targetLength / padString.length);
        }
        return padString.slice(0, targetLength) + String(this);
    };
}

// String.prototype.repeat polyfill
if (!String.prototype.repeat) {
    String.prototype.repeat = function(count) {
        if (this == null) throw new TypeError("can't convert " + this + " to object");
        var str = '' + this;
        count = +count;
        if (count !== count) count = 0;
        if (count < 0) throw new RangeError('repeat count must be non-negative');
        if (count === Infinity) throw new RangeError('repeat count must be less than infinity');
        count = Math.floor(count);
        if (str.length === 0 || count === 0) return '';
        if (str.length * count >= 1 << 28) throw new RangeError('repeat count must not overflow maximum string size');
        var rpt = '';
        for (var i = 0; i < count; i++) {
            rpt += str;
        }
        return rpt;
    };
}

// Element.prototype.classList polyfill check
if (!("classList" in document.documentElement)) {
    Object.defineProperty(HTMLElement.prototype, 'classList', {
        get: function() {
            var self = this;
            function update(fn) {
                return function(value) {
                    var classes = self.className.split(/\s+/g);
                    var index = classes.indexOf(value);
                    fn(classes, index, value);
                    self.className = classes.join(' ');
                };
            }
            return {
                add: update(function(classes, index, value) {
                    if (!~index) classes.push(value);
                }),
                remove: update(function(classes, index) {
                    if (~index) classes.splice(index, 1);
                }),
                toggle: update(function(classes, index, value) {
                    if (~index) classes.splice(index, 1);
                    else classes.push(value);
                }),
                contains: function(value) {
                    return !!~self.className.split(/\s+/g).indexOf(value);
                }
            };
        }
    });
}

// Number.prototype.toLocaleString fallback for older browsers
(function() {
    var orig = Number.prototype.toLocaleString;
    Number.prototype.toLocaleString = function() {
        try {
            return orig.apply(this, arguments);
        } catch (e) {
            // Fallback: simple comma formatting
            return this.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
        }
    };
})();
