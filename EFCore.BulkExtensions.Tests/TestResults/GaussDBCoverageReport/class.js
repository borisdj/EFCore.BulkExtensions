/* Chartist.js 0.11.4
 * Copyright © 2019 Gion Kunz
 * Free to use under either the WTFPL license or the MIT license.
 * https://raw.githubusercontent.com/gionkunz/chartist-js/master/LICENSE-WTFPL
 * https://raw.githubusercontent.com/gionkunz/chartist-js/master/LICENSE-MIT
 */

!function (e, t) { "function" == typeof define && define.amd ? define("Chartist", [], (function () { return e.Chartist = t() })) : "object" == typeof module && module.exports ? module.exports = t() : e.Chartist = t() }(this, (function () { var e = { version: "0.11.4" }; return function (e, t) { "use strict"; var i = e.window, n = e.document; t.namespaces = { svg: "http://www.w3.org/2000/svg", xmlns: "http://www.w3.org/2000/xmlns/", xhtml: "http://www.w3.org/1999/xhtml", xlink: "http://www.w3.org/1999/xlink", ct: "http://gionkunz.github.com/chartist-js/ct" }, t.noop = function (e) { return e }, t.alphaNumerate = function (e) { return String.fromCharCode(97 + e % 26) }, t.extend = function (e) { var i, n, s, r; for (e = e || {}, i = 1; i < arguments.length; i++)for (var a in n = arguments[i], r = Object.getPrototypeOf(e), n) "__proto__" === a || "constructor" === a || null !== r && a in r || (s = n[a], e[a] = "object" != typeof s || null === s || s instanceof Array ? s : t.extend(e[a], s)); return e }, t.replaceAll = function (e, t, i) { return e.replace(new RegExp(t, "g"), i) }, t.ensureUnit = function (e, t) { return "number" == typeof e && (e += t), e }, t.quantity = function (e) { if ("string" == typeof e) { var t = /^(\d+)\s*(.*)$/g.exec(e); return { value: +t[1], unit: t[2] || void 0 } } return { value: e } }, t.querySelector = function (e) { return e instanceof Node ? e : n.querySelector(e) }, t.times = function (e) { return Array.apply(null, new Array(e)) }, t.sum = function (e, t) { return e + (t || 0) }, t.mapMultiply = function (e) { return function (t) { return t * e } }, t.mapAdd = function (e) { return function (t) { return t + e } }, t.serialMap = function (e, i) { var n = [], s = Math.max.apply(null, e.map((function (e) { return e.length }))); return t.times(s).forEach((function (t, s) { var r = e.map((function (e) { return e[s] })); n[s] = i.apply(null, r) })), n }, t.roundWithPrecision = function (e, i) { var n = Math.pow(10, i || t.precision); return Math.round(e * n) / n }, t.precision = 8, t.escapingMap = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }, t.serialize = function (e) { return null == e ? e : ("number" == typeof e ? e = "" + e : "object" == typeof e && (e = JSON.stringify({ data: e })), Object.keys(t.escapingMap).reduce((function (e, i) { return t.replaceAll(e, i, t.escapingMap[i]) }), e)) }, t.deserialize = function (e) { if ("string" != typeof e) return e; e = Object.keys(t.escapingMap).reduce((function (e, i) { return t.replaceAll(e, t.escapingMap[i], i) }), e); try { e = void 0 !== (e = JSON.parse(e)).data ? e.data : e } catch (e) { } return e }, t.createSvg = function (e, i, n, s) { var r; return i = i || "100%", n = n || "100%", Array.prototype.slice.call(e.querySelectorAll("svg")).filter((function (e) { return e.getAttributeNS(t.namespaces.xmlns, "ct") })).forEach((function (t) { e.removeChild(t) })), (r = new t.Svg("svg").attr({ width: i, height: n }).addClass(s))._node.style.width = i, r._node.style.height = n, e.appendChild(r._node), r }, t.normalizeData = function (e, i, n) { var s, r = { raw: e, normalized: {} }; return r.normalized.series = t.getDataArray({ series: e.series || [] }, i, n), s = r.normalized.series.every((function (e) { return e instanceof Array })) ? Math.max.apply(null, r.normalized.series.map((function (e) { return e.length }))) : r.normalized.series.length, r.normalized.labels = (e.labels || []).slice(), Array.prototype.push.apply(r.normalized.labels, t.times(Math.max(0, s - r.normalized.labels.length)).map((function () { return "" }))), i && t.reverseData(r.normalized), r }, t.safeHasProperty = function (e, t) { return null !== e && "object" == typeof e && e.hasOwnProperty(t) }, t.isDataHoleValue = function (e) { return null == e || "number" == typeof e && isNaN(e) }, t.reverseData = function (e) { e.labels.reverse(), e.series.reverse(); for (var t = 0; t < e.series.length; t++)"object" == typeof e.series[t] && void 0 !== e.series[t].data ? e.series[t].data.reverse() : e.series[t] instanceof Array && e.series[t].reverse() }, t.getDataArray = function (e, i, n) { return e.series.map((function e(i) { if (t.safeHasProperty(i, "value")) return e(i.value); if (t.safeHasProperty(i, "data")) return e(i.data); if (i instanceof Array) return i.map(e); if (!t.isDataHoleValue(i)) { if (n) { var s = {}; return "string" == typeof n ? s[n] = t.getNumberOrUndefined(i) : s.y = t.getNumberOrUndefined(i), s.x = i.hasOwnProperty("x") ? t.getNumberOrUndefined(i.x) : s.x, s.y = i.hasOwnProperty("y") ? t.getNumberOrUndefined(i.y) : s.y, s } return t.getNumberOrUndefined(i) } })) }, t.normalizePadding = function (e, t) { return t = t || 0, "number" == typeof e ? { top: e, right: e, bottom: e, left: e } : { top: "number" == typeof e.top ? e.top : t, right: "number" == typeof e.right ? e.right : t, bottom: "number" == typeof e.bottom ? e.bottom : t, left: "number" == typeof e.left ? e.left : t } }, t.getMetaData = function (e, t) { var i = e.data ? e.data[t] : e[t]; return i ? i.meta : void 0 }, t.orderOfMagnitude = function (e) { return Math.floor(Math.log(Math.abs(e)) / Math.LN10) }, t.projectLength = function (e, t, i) { return t / i.range * e }, t.getAvailableHeight = function (e, i) { return Math.max((t.quantity(i.height).value || e.height()) - (i.chartPadding.top + i.chartPadding.bottom) - i.axisX.offset, 0) }, t.getHighLow = function (e, i, n) { var s = { high: void 0 === (i = t.extend({}, i, n ? i["axis" + n.toUpperCase()] : {})).high ? -Number.MAX_VALUE : +i.high, low: void 0 === i.low ? Number.MAX_VALUE : +i.low }, r = void 0 === i.high, a = void 0 === i.low; return (r || a) && function e(t) { if (void 0 !== t) if (t instanceof Array) for (var i = 0; i < t.length; i++)e(t[i]); else { var o = n ? +t[n] : +t; r && o > s.high && (s.high = o), a && o < s.low && (s.low = o) } }(e), (i.referenceValue || 0 === i.referenceValue) && (s.high = Math.max(i.referenceValue, s.high), s.low = Math.min(i.referenceValue, s.low)), s.high <= s.low && (0 === s.low ? s.high = 1 : s.low < 0 ? s.high = 0 : (s.high > 0 || (s.high = 1), s.low = 0)), s }, t.isNumeric = function (e) { return null !== e && isFinite(e) }, t.isFalseyButZero = function (e) { return !e && 0 !== e }, t.getNumberOrUndefined = function (e) { return t.isNumeric(e) ? +e : void 0 }, t.isMultiValue = function (e) { return "object" == typeof e && ("x" in e || "y" in e) }, t.getMultiValue = function (e, i) { return t.isMultiValue(e) ? t.getNumberOrUndefined(e[i || "y"]) : t.getNumberOrUndefined(e) }, t.rho = function (e) { if (1 === e) return e; function t(e, i) { return e % i == 0 ? i : t(i, e % i) } function i(e) { return e * e + 1 } var n, s = 2, r = 2; if (e % 2 == 0) return 2; do { s = i(s) % e, r = i(i(r)) % e, n = t(Math.abs(s - r), e) } while (1 === n); return n }, t.getBounds = function (e, i, n, s) { var r, a, o, l = 0, h = { high: i.high, low: i.low }; h.valueRange = h.high - h.low, h.oom = t.orderOfMagnitude(h.valueRange), h.step = Math.pow(10, h.oom), h.min = Math.floor(h.low / h.step) * h.step, h.max = Math.ceil(h.high / h.step) * h.step, h.range = h.max - h.min, h.numberOfSteps = Math.round(h.range / h.step); var u = t.projectLength(e, h.step, h) < n, c = s ? t.rho(h.range) : 0; if (s && t.projectLength(e, 1, h) >= n) h.step = 1; else if (s && c < h.step && t.projectLength(e, c, h) >= n) h.step = c; else for (; ;) { if (u && t.projectLength(e, h.step, h) <= n) h.step *= 2; else { if (u || !(t.projectLength(e, h.step / 2, h) >= n)) break; if (h.step /= 2, s && h.step % 1 != 0) { h.step *= 2; break } } if (l++ > 1e3) throw new Error("Exceeded maximum number of iterations while optimizing scale step!") } var d = 2221e-19; function p(e, t) { return e === (e += t) && (e *= 1 + (t > 0 ? d : -d)), e } for (h.step = Math.max(h.step, d), a = h.min, o = h.max; a + h.step <= h.low;)a = p(a, h.step); for (; o - h.step >= h.high;)o = p(o, -h.step); h.min = a, h.max = o, h.range = h.max - h.min; var f = []; for (r = h.min; r <= h.max; r = p(r, h.step)) { var m = t.roundWithPrecision(r); m !== f[f.length - 1] && f.push(m) } return h.values = f, h }, t.polarToCartesian = function (e, t, i, n) { var s = (n - 90) * Math.PI / 180; return { x: e + i * Math.cos(s), y: t + i * Math.sin(s) } }, t.createChartRect = function (e, i, n) { var s = !(!i.axisX && !i.axisY), r = s ? i.axisY.offset : 0, a = s ? i.axisX.offset : 0, o = e.width() || t.quantity(i.width).value || 0, l = e.height() || t.quantity(i.height).value || 0, h = t.normalizePadding(i.chartPadding, n); o = Math.max(o, r + h.left + h.right), l = Math.max(l, a + h.top + h.bottom); var u = { padding: h, width: function () { return this.x2 - this.x1 }, height: function () { return this.y1 - this.y2 } }; return s ? ("start" === i.axisX.position ? (u.y2 = h.top + a, u.y1 = Math.max(l - h.bottom, u.y2 + 1)) : (u.y2 = h.top, u.y1 = Math.max(l - h.bottom - a, u.y2 + 1)), "start" === i.axisY.position ? (u.x1 = h.left + r, u.x2 = Math.max(o - h.right, u.x1 + 1)) : (u.x1 = h.left, u.x2 = Math.max(o - h.right - r, u.x1 + 1))) : (u.x1 = h.left, u.x2 = Math.max(o - h.right, u.x1 + 1), u.y2 = h.top, u.y1 = Math.max(l - h.bottom, u.y2 + 1)), u }, t.createGrid = function (e, i, n, s, r, a, o, l) { var h = {}; h[n.units.pos + "1"] = e, h[n.units.pos + "2"] = e, h[n.counterUnits.pos + "1"] = s, h[n.counterUnits.pos + "2"] = s + r; var u = a.elem("line", h, o.join(" ")); l.emit("draw", t.extend({ type: "grid", axis: n, index: i, group: a, element: u }, h)) }, t.createGridBackground = function (e, t, i, n) { var s = e.elem("rect", { x: t.x1, y: t.y2, width: t.width(), height: t.height() }, i, !0); n.emit("draw", { type: "gridBackground", group: e, element: s }) }, t.createLabel = function (e, i, s, r, a, o, l, h, u, c, d) { var p, f = {}; if (f[a.units.pos] = e + l[a.units.pos], f[a.counterUnits.pos] = l[a.counterUnits.pos], f[a.units.len] = i, f[a.counterUnits.len] = Math.max(0, o - 10), c) { var m = n.createElement("span"); m.className = u.join(" "), m.setAttribute("xmlns", t.namespaces.xhtml), m.innerText = r[s], m.style[a.units.len] = Math.round(f[a.units.len]) + "px", m.style[a.counterUnits.len] = Math.round(f[a.counterUnits.len]) + "px", p = h.foreignObject(m, t.extend({ style: "overflow: visible;" }, f)) } else p = h.elem("text", f, u.join(" ")).text(r[s]); d.emit("draw", t.extend({ type: "label", axis: a, index: s, group: h, element: p, text: r[s] }, f)) }, t.getSeriesOption = function (e, t, i) { if (e.name && t.series && t.series[e.name]) { var n = t.series[e.name]; return n.hasOwnProperty(i) ? n[i] : t[i] } return t[i] }, t.optionsProvider = function (e, n, s) { var r, a, o = t.extend({}, e), l = []; function h(e) { var l = r; if (r = t.extend({}, o), n) for (a = 0; a < n.length; a++) { i.matchMedia(n[a][0]).matches && (r = t.extend(r, n[a][1])) } s && e && s.emit("optionsChanged", { previousOptions: l, currentOptions: r }) } if (!i.matchMedia) throw "window.matchMedia not found! Make sure you're using a polyfill."; if (n) for (a = 0; a < n.length; a++) { var u = i.matchMedia(n[a][0]); u.addListener(h), l.push(u) } return h(), { removeMediaQueryListeners: function () { l.forEach((function (e) { e.removeListener(h) })) }, getCurrentOptions: function () { return t.extend({}, r) } } }, t.splitIntoSegments = function (e, i, n) { n = t.extend({}, { increasingX: !1, fillHoles: !1 }, n); for (var s = [], r = !0, a = 0; a < e.length; a += 2)void 0 === t.getMultiValue(i[a / 2].value) ? n.fillHoles || (r = !0) : (n.increasingX && a >= 2 && e[a] <= e[a - 2] && (r = !0), r && (s.push({ pathCoordinates: [], valueData: [] }), r = !1), s[s.length - 1].pathCoordinates.push(e[a], e[a + 1]), s[s.length - 1].valueData.push(i[a / 2])); return s } }(this || global, e), function (e, t) { "use strict"; t.Interpolation = {}, t.Interpolation.none = function (e) { return e = t.extend({}, { fillHoles: !1 }, e), function (i, n) { for (var s = new t.Svg.Path, r = !0, a = 0; a < i.length; a += 2) { var o = i[a], l = i[a + 1], h = n[a / 2]; void 0 !== t.getMultiValue(h.value) ? (r ? s.move(o, l, !1, h) : s.line(o, l, !1, h), r = !1) : e.fillHoles || (r = !0) } return s } }, t.Interpolation.simple = function (e) { e = t.extend({}, { divisor: 2, fillHoles: !1 }, e); var i = 1 / Math.max(1, e.divisor); return function (n, s) { for (var r, a, o, l = new t.Svg.Path, h = 0; h < n.length; h += 2) { var u = n[h], c = n[h + 1], d = (u - r) * i, p = s[h / 2]; void 0 !== p.value ? (void 0 === o ? l.move(u, c, !1, p) : l.curve(r + d, a, u - d, c, u, c, !1, p), r = u, a = c, o = p) : e.fillHoles || (r = u = o = void 0) } return l } }, t.Interpolation.cardinal = function (e) { e = t.extend({}, { tension: 1, fillHoles: !1 }, e); var i = Math.min(1, Math.max(0, e.tension)), n = 1 - i; return function s(r, a) { var o = t.splitIntoSegments(r, a, { fillHoles: e.fillHoles }); if (o.length) { if (o.length > 1) { var l = []; return o.forEach((function (e) { l.push(s(e.pathCoordinates, e.valueData)) })), t.Svg.Path.join(l) } if (r = o[0].pathCoordinates, a = o[0].valueData, r.length <= 4) return t.Interpolation.none()(r, a); for (var h = (new t.Svg.Path).move(r[0], r[1], !1, a[0]), u = 0, c = r.length; c - 2 > u; u += 2) { var d = [{ x: +r[u - 2], y: +r[u - 1] }, { x: +r[u], y: +r[u + 1] }, { x: +r[u + 2], y: +r[u + 3] }, { x: +r[u + 4], y: +r[u + 5] }]; c - 4 === u ? d[3] = d[2] : u || (d[0] = { x: +r[u], y: +r[u + 1] }), h.curve(i * (-d[0].x + 6 * d[1].x + d[2].x) / 6 + n * d[2].x, i * (-d[0].y + 6 * d[1].y + d[2].y) / 6 + n * d[2].y, i * (d[1].x + 6 * d[2].x - d[3].x) / 6 + n * d[2].x, i * (d[1].y + 6 * d[2].y - d[3].y) / 6 + n * d[2].y, d[2].x, d[2].y, !1, a[(u + 2) / 2]) } return h } return t.Interpolation.none()([]) } }, t.Interpolation.monotoneCubic = function (e) { return e = t.extend({}, { fillHoles: !1 }, e), function i(n, s) { var r = t.splitIntoSegments(n, s, { fillHoles: e.fillHoles, increasingX: !0 }); if (r.length) { if (r.length > 1) { var a = []; return r.forEach((function (e) { a.push(i(e.pathCoordinates, e.valueData)) })), t.Svg.Path.join(a) } if (n = r[0].pathCoordinates, s = r[0].valueData, n.length <= 4) return t.Interpolation.none()(n, s); var o, l, h = [], u = [], c = n.length / 2, d = [], p = [], f = [], m = []; for (o = 0; o < c; o++)h[o] = n[2 * o], u[o] = n[2 * o + 1]; for (o = 0; o < c - 1; o++)f[o] = u[o + 1] - u[o], m[o] = h[o + 1] - h[o], p[o] = f[o] / m[o]; for (d[0] = p[0], d[c - 1] = p[c - 2], o = 1; o < c - 1; o++)0 === p[o] || 0 === p[o - 1] || p[o - 1] > 0 != p[o] > 0 ? d[o] = 0 : (d[o] = 3 * (m[o - 1] + m[o]) / ((2 * m[o] + m[o - 1]) / p[o - 1] + (m[o] + 2 * m[o - 1]) / p[o]), isFinite(d[o]) || (d[o] = 0)); for (l = (new t.Svg.Path).move(h[0], u[0], !1, s[0]), o = 0; o < c - 1; o++)l.curve(h[o] + m[o] / 3, u[o] + d[o] * m[o] / 3, h[o + 1] - m[o] / 3, u[o + 1] - d[o + 1] * m[o] / 3, h[o + 1], u[o + 1], !1, s[o + 1]); return l } return t.Interpolation.none()([]) } }, t.Interpolation.step = function (e) { return e = t.extend({}, { postpone: !0, fillHoles: !1 }, e), function (i, n) { for (var s, r, a, o = new t.Svg.Path, l = 0; l < i.length; l += 2) { var h = i[l], u = i[l + 1], c = n[l / 2]; void 0 !== c.value ? (void 0 === a ? o.move(h, u, !1, c) : (e.postpone ? o.line(h, r, !1, a) : o.line(s, u, !1, c), o.line(h, u, !1, c)), s = h, r = u, a = c) : e.fillHoles || (s = r = a = void 0) } return o } } }(this || global, e), function (e, t) { "use strict"; t.EventEmitter = function () { var e = []; return { addEventHandler: function (t, i) { e[t] = e[t] || [], e[t].push(i) }, removeEventHandler: function (t, i) { e[t] && (i ? (e[t].splice(e[t].indexOf(i), 1), 0 === e[t].length && delete e[t]) : delete e[t]) }, emit: function (t, i) { e[t] && e[t].forEach((function (e) { e(i) })), e["*"] && e["*"].forEach((function (e) { e(t, i) })) } } } }(this || global, e), function (e, t) { "use strict"; t.Class = { extend: function (e, i) { var n = i || this.prototype || t.Class, s = Object.create(n); t.Class.cloneDefinitions(s, e); var r = function () { var e, i = s.constructor || function () { }; return e = this === t ? Object.create(s) : this, i.apply(e, Array.prototype.slice.call(arguments, 0)), e }; return r.prototype = s, r.super = n, r.extend = this.extend, r }, cloneDefinitions: function () { var e = function (e) { var t = []; if (e.length) for (var i = 0; i < e.length; i++)t.push(e[i]); return t }(arguments), t = e[0]; return e.splice(1, e.length - 1).forEach((function (e) { Object.getOwnPropertyNames(e).forEach((function (i) { delete t[i], Object.defineProperty(t, i, Object.getOwnPropertyDescriptor(e, i)) })) })), t } } }(this || global, e), function (e, t) { "use strict"; var i = e.window; function n() { i.addEventListener("resize", this.resizeListener), this.optionsProvider = t.optionsProvider(this.options, this.responsiveOptions, this.eventEmitter), this.eventEmitter.addEventHandler("optionsChanged", function () { this.update() }.bind(this)), this.options.plugins && this.options.plugins.forEach(function (e) { e instanceof Array ? e[0](this, e[1]) : e(this) }.bind(this)), this.eventEmitter.emit("data", { type: "initial", data: this.data }), this.createChart(this.optionsProvider.getCurrentOptions()), this.initializeTimeoutId = void 0 } t.Base = t.Class.extend({ constructor: function (e, i, s, r, a) { this.container = t.querySelector(e), this.data = i || {}, this.data.labels = this.data.labels || [], this.data.series = this.data.series || [], this.defaultOptions = s, this.options = r, this.responsiveOptions = a, this.eventEmitter = t.EventEmitter(), this.supportsForeignObject = t.Svg.isSupported("Extensibility"), this.supportsAnimations = t.Svg.isSupported("AnimationEventsAttribute"), this.resizeListener = function () { this.update() }.bind(this), this.container && (this.container.__chartist__ && this.container.__chartist__.detach(), this.container.__chartist__ = this), this.initializeTimeoutId = setTimeout(n.bind(this), 0) }, optionsProvider: void 0, container: void 0, svg: void 0, eventEmitter: void 0, createChart: function () { throw new Error("Base chart type can't be instantiated!") }, update: function (e, i, n) { return e && (this.data = e || {}, this.data.labels = this.data.labels || [], this.data.series = this.data.series || [], this.eventEmitter.emit("data", { type: "update", data: this.data })), i && (this.options = t.extend({}, n ? this.options : this.defaultOptions, i), this.initializeTimeoutId || (this.optionsProvider.removeMediaQueryListeners(), this.optionsProvider = t.optionsProvider(this.options, this.responsiveOptions, this.eventEmitter))), this.initializeTimeoutId || this.createChart(this.optionsProvider.getCurrentOptions()), this }, detach: function () { return this.initializeTimeoutId ? i.clearTimeout(this.initializeTimeoutId) : (i.removeEventListener("resize", this.resizeListener), this.optionsProvider.removeMediaQueryListeners()), this }, on: function (e, t) { return this.eventEmitter.addEventHandler(e, t), this }, off: function (e, t) { return this.eventEmitter.removeEventHandler(e, t), this }, version: t.version, supportsForeignObject: !1 }) }(this || global, e), function (e, t) { "use strict"; var i = e.document; t.Svg = t.Class.extend({ constructor: function (e, n, s, r, a) { e instanceof Element ? this._node = e : (this._node = i.createElementNS(t.namespaces.svg, e), "svg" === e && this.attr({ "xmlns:ct": t.namespaces.ct })), n && this.attr(n), s && this.addClass(s), r && (a && r._node.firstChild ? r._node.insertBefore(this._node, r._node.firstChild) : r._node.appendChild(this._node)) }, attr: function (e, i) { return "string" == typeof e ? i ? this._node.getAttributeNS(i, e) : this._node.getAttribute(e) : (Object.keys(e).forEach(function (i) { if (void 0 !== e[i]) if (-1 !== i.indexOf(":")) { var n = i.split(":"); this._node.setAttributeNS(t.namespaces[n[0]], i, e[i]) } else this._node.setAttribute(i, e[i]) }.bind(this)), this) }, elem: function (e, i, n, s) { return new t.Svg(e, i, n, this, s) }, parent: function () { return this._node.parentNode instanceof SVGElement ? new t.Svg(this._node.parentNode) : null }, root: function () { for (var e = this._node; "svg" !== e.nodeName;)e = e.parentNode; return new t.Svg(e) }, querySelector: function (e) { var i = this._node.querySelector(e); return i ? new t.Svg(i) : null }, querySelectorAll: function (e) { var i = this._node.querySelectorAll(e); return i.length ? new t.Svg.List(i) : null }, getNode: function () { return this._node }, foreignObject: function (e, n, s, r) { if ("string" == typeof e) { var a = i.createElement("div"); a.innerHTML = e, e = a.firstChild } e.setAttribute("xmlns", t.namespaces.xmlns); var o = this.elem("foreignObject", n, s, r); return o._node.appendChild(e), o }, text: function (e) { return this._node.appendChild(i.createTextNode(e)), this }, empty: function () { for (; this._node.firstChild;)this._node.removeChild(this._node.firstChild); return this }, remove: function () { return this._node.parentNode.removeChild(this._node), this.parent() }, replace: function (e) { return this._node.parentNode.replaceChild(e._node, this._node), e }, append: function (e, t) { return t && this._node.firstChild ? this._node.insertBefore(e._node, this._node.firstChild) : this._node.appendChild(e._node), this }, classes: function () { return this._node.getAttribute("class") ? this._node.getAttribute("class").trim().split(/\s+/) : [] }, addClass: function (e) { return this._node.setAttribute("class", this.classes(this._node).concat(e.trim().split(/\s+/)).filter((function (e, t, i) { return i.indexOf(e) === t })).join(" ")), this }, removeClass: function (e) { var t = e.trim().split(/\s+/); return this._node.setAttribute("class", this.classes(this._node).filter((function (e) { return -1 === t.indexOf(e) })).join(" ")), this }, removeAllClasses: function () { return this._node.setAttribute("class", ""), this }, height: function () { return this._node.getBoundingClientRect().height }, width: function () { return this._node.getBoundingClientRect().width }, animate: function (e, i, n) { return void 0 === i && (i = !0), Object.keys(e).forEach(function (s) { function r(e, i) { var r, a, o, l = {}; e.easing && (o = e.easing instanceof Array ? e.easing : t.Svg.Easing[e.easing], delete e.easing), e.begin = t.ensureUnit(e.begin, "ms"), e.dur = t.ensureUnit(e.dur, "ms"), o && (e.calcMode = "spline", e.keySplines = o.join(" "), e.keyTimes = "0;1"), i && (e.fill = "freeze", l[s] = e.from, this.attr(l), a = t.quantity(e.begin || 0).value, e.begin = "indefinite"), r = this.elem("animate", t.extend({ attributeName: s }, e)), i && setTimeout(function () { try { r._node.beginElement() } catch (t) { l[s] = e.to, this.attr(l), r.remove() } }.bind(this), a), n && r._node.addEventListener("beginEvent", function () { n.emit("animationBegin", { element: this, animate: r._node, params: e }) }.bind(this)), r._node.addEventListener("endEvent", function () { n && n.emit("animationEnd", { element: this, animate: r._node, params: e }), i && (l[s] = e.to, this.attr(l), r.remove()) }.bind(this)) } e[s] instanceof Array ? e[s].forEach(function (e) { r.bind(this)(e, !1) }.bind(this)) : r.bind(this)(e[s], i) }.bind(this)), this } }), t.Svg.isSupported = function (e) { return i.implementation.hasFeature("http://www.w3.org/TR/SVG11/feature#" + e, "1.1") }; t.Svg.Easing = { easeInSine: [.47, 0, .745, .715], easeOutSine: [.39, .575, .565, 1], easeInOutSine: [.445, .05, .55, .95], easeInQuad: [.55, .085, .68, .53], easeOutQuad: [.25, .46, .45, .94], easeInOutQuad: [.455, .03, .515, .955], easeInCubic: [.55, .055, .675, .19], easeOutCubic: [.215, .61, .355, 1], easeInOutCubic: [.645, .045, .355, 1], easeInQuart: [.895, .03, .685, .22], easeOutQuart: [.165, .84, .44, 1], easeInOutQuart: [.77, 0, .175, 1], easeInQuint: [.755, .05, .855, .06], easeOutQuint: [.23, 1, .32, 1], easeInOutQuint: [.86, 0, .07, 1], easeInExpo: [.95, .05, .795, .035], easeOutExpo: [.19, 1, .22, 1], easeInOutExpo: [1, 0, 0, 1], easeInCirc: [.6, .04, .98, .335], easeOutCirc: [.075, .82, .165, 1], easeInOutCirc: [.785, .135, .15, .86], easeInBack: [.6, -.28, .735, .045], easeOutBack: [.175, .885, .32, 1.275], easeInOutBack: [.68, -.55, .265, 1.55] }, t.Svg.List = t.Class.extend({ constructor: function (e) { var i = this; this.svgElements = []; for (var n = 0; n < e.length; n++)this.svgElements.push(new t.Svg(e[n])); Object.keys(t.Svg.prototype).filter((function (e) { return -1 === ["constructor", "parent", "querySelector", "querySelectorAll", "replace", "append", "classes", "height", "width"].indexOf(e) })).forEach((function (e) { i[e] = function () { var n = Array.prototype.slice.call(arguments, 0); return i.svgElements.forEach((function (i) { t.Svg.prototype[e].apply(i, n) })), i } })) } }) }(this || global, e), function (e, t) { "use strict"; var i = { m: ["x", "y"], l: ["x", "y"], c: ["x1", "y1", "x2", "y2", "x", "y"], a: ["rx", "ry", "xAr", "lAf", "sf", "x", "y"] }, n = { accuracy: 3 }; function s(e, i, n, s, r, a) { var o = t.extend({ command: r ? e.toLowerCase() : e.toUpperCase() }, i, a ? { data: a } : {}); n.splice(s, 0, o) } function r(e, t) { e.forEach((function (n, s) { i[n.command.toLowerCase()].forEach((function (i, r) { t(n, i, s, r, e) })) })) } t.Svg.Path = t.Class.extend({ constructor: function (e, i) { this.pathElements = [], this.pos = 0, this.close = e, this.options = t.extend({}, n, i) }, position: function (e) { return void 0 !== e ? (this.pos = Math.max(0, Math.min(this.pathElements.length, e)), this) : this.pos }, remove: function (e) { return this.pathElements.splice(this.pos, e), this }, move: function (e, t, i, n) { return s("M", { x: +e, y: +t }, this.pathElements, this.pos++, i, n), this }, line: function (e, t, i, n) { return s("L", { x: +e, y: +t }, this.pathElements, this.pos++, i, n), this }, curve: function (e, t, i, n, r, a, o, l) { return s("C", { x1: +e, y1: +t, x2: +i, y2: +n, x: +r, y: +a }, this.pathElements, this.pos++, o, l), this }, arc: function (e, t, i, n, r, a, o, l, h) { return s("A", { rx: +e, ry: +t, xAr: +i, lAf: +n, sf: +r, x: +a, y: +o }, this.pathElements, this.pos++, l, h), this }, scale: function (e, t) { return r(this.pathElements, (function (i, n) { i[n] *= "x" === n[0] ? e : t })), this }, translate: function (e, t) { return r(this.pathElements, (function (i, n) { i[n] += "x" === n[0] ? e : t })), this }, transform: function (e) { return r(this.pathElements, (function (t, i, n, s, r) { var a = e(t, i, n, s, r); (a || 0 === a) && (t[i] = a) })), this }, parse: function (e) { var n = e.replace(/([A-Za-z])([0-9])/g, "$1 $2").replace(/([0-9])([A-Za-z])/g, "$1 $2").split(/[\s,]+/).reduce((function (e, t) { return t.match(/[A-Za-z]/) && e.push([]), e[e.length - 1].push(t), e }), []); "Z" === n[n.length - 1][0].toUpperCase() && n.pop(); var s = n.map((function (e) { var n = e.shift(), s = i[n.toLowerCase()]; return t.extend({ command: n }, s.reduce((function (t, i, n) { return t[i] = +e[n], t }), {})) })), r = [this.pos, 0]; return Array.prototype.push.apply(r, s), Array.prototype.splice.apply(this.pathElements, r), this.pos += s.length, this }, stringify: function () { var e = Math.pow(10, this.options.accuracy); return this.pathElements.reduce(function (t, n) { var s = i[n.command.toLowerCase()].map(function (t) { return this.options.accuracy ? Math.round(n[t] * e) / e : n[t] }.bind(this)); return t + n.command + s.join(",") }.bind(this), "") + (this.close ? "Z" : "") }, clone: function (e) { var i = new t.Svg.Path(e || this.close); return i.pos = this.pos, i.pathElements = this.pathElements.slice().map((function (e) { return t.extend({}, e) })), i.options = t.extend({}, this.options), i }, splitByCommand: function (e) { var i = [new t.Svg.Path]; return this.pathElements.forEach((function (n) { n.command === e.toUpperCase() && 0 !== i[i.length - 1].pathElements.length && i.push(new t.Svg.Path), i[i.length - 1].pathElements.push(n) })), i } }), t.Svg.Path.elementDescriptions = i, t.Svg.Path.join = function (e, i, n) { for (var s = new t.Svg.Path(i, n), r = 0; r < e.length; r++)for (var a = e[r], o = 0; o < a.pathElements.length; o++)s.pathElements.push(a.pathElements[o]); return s } }(this || global, e), function (e, t) { "use strict"; e.window, e.document; var i = { x: { pos: "x", len: "width", dir: "horizontal", rectStart: "x1", rectEnd: "x2", rectOffset: "y2" }, y: { pos: "y", len: "height", dir: "vertical", rectStart: "y2", rectEnd: "y1", rectOffset: "x1" } }; t.Axis = t.Class.extend({ constructor: function (e, t, n, s) { this.units = e, this.counterUnits = e === i.x ? i.y : i.x, this.chartRect = t, this.axisLength = t[e.rectEnd] - t[e.rectStart], this.gridOffset = t[e.rectOffset], this.ticks = n, this.options = s }, createGridAndLabels: function (e, i, n, s, r) { var a = s["axis" + this.units.pos.toUpperCase()], o = this.ticks.map(this.projectValue.bind(this)), l = this.ticks.map(a.labelInterpolationFnc); o.forEach(function (h, u) { var c, d = { x: 0, y: 0 }; c = o[u + 1] ? o[u + 1] - h : Math.max(this.axisLength - h, 30), t.isFalseyButZero(l[u]) && "" !== l[u] || ("x" === this.units.pos ? (h = this.chartRect.x1 + h, d.x = s.axisX.labelOffset.x, "start" === s.axisX.position ? d.y = this.chartRect.padding.top + s.axisX.labelOffset.y + (n ? 5 : 20) : d.y = this.chartRect.y1 + s.axisX.labelOffset.y + (n ? 5 : 20)) : (h = this.chartRect.y1 - h, d.y = s.axisY.labelOffset.y - (n ? c : 0), "start" === s.axisY.position ? d.x = n ? this.chartRect.padding.left + s.axisY.labelOffset.x : this.chartRect.x1 - 10 : d.x = this.chartRect.x2 + s.axisY.labelOffset.x + 10), a.showGrid && t.createGrid(h, u, this, this.gridOffset, this.chartRect[this.counterUnits.len](), e, [s.classNames.grid, s.classNames[this.units.dir]], r), a.showLabel && t.createLabel(h, c, u, l, this, a.offset, d, i, [s.classNames.label, s.classNames[this.units.dir], "start" === a.position ? s.classNames[a.position] : s.classNames.end], n, r)) }.bind(this)) }, projectValue: function (e, t, i) { throw new Error("Base axis can't be instantiated!") } }), t.Axis.units = i }(this || global, e), function (e, t) { "use strict"; e.window, e.document; t.AutoScaleAxis = t.Axis.extend({ constructor: function (e, i, n, s) { var r = s.highLow || t.getHighLow(i, s, e.pos); this.bounds = t.getBounds(n[e.rectEnd] - n[e.rectStart], r, s.scaleMinSpace || 20, s.onlyInteger), this.range = { min: this.bounds.min, max: this.bounds.max }, t.AutoScaleAxis.super.constructor.call(this, e, n, this.bounds.values, s) }, projectValue: function (e) { return this.axisLength * (+t.getMultiValue(e, this.units.pos) - this.bounds.min) / this.bounds.range } }) }(this || global, e), function (e, t) { "use strict"; e.window, e.document; t.FixedScaleAxis = t.Axis.extend({ constructor: function (e, i, n, s) { var r = s.highLow || t.getHighLow(i, s, e.pos); this.divisor = s.divisor || 1, this.ticks = s.ticks || t.times(this.divisor).map(function (e, t) { return r.low + (r.high - r.low) / this.divisor * t }.bind(this)), this.ticks.sort((function (e, t) { return e - t })), this.range = { min: r.low, max: r.high }, t.FixedScaleAxis.super.constructor.call(this, e, n, this.ticks, s), this.stepLength = this.axisLength / this.divisor }, projectValue: function (e) { return this.axisLength * (+t.getMultiValue(e, this.units.pos) - this.range.min) / (this.range.max - this.range.min) } }) }(this || global, e), function (e, t) { "use strict"; e.window, e.document; t.StepAxis = t.Axis.extend({ constructor: function (e, i, n, s) { t.StepAxis.super.constructor.call(this, e, n, s.ticks, s); var r = Math.max(1, s.ticks.length - (s.stretch ? 1 : 0)); this.stepLength = this.axisLength / r }, projectValue: function (e, t) { return this.stepLength * t } }) }(this || global, e), function (e, t) { "use strict"; e.window, e.document; var i = { axisX: { offset: 30, position: "end", labelOffset: { x: 0, y: 0 }, showLabel: !0, showGrid: !0, labelInterpolationFnc: t.noop, type: void 0 }, axisY: { offset: 40, position: "start", labelOffset: { x: 0, y: 0 }, showLabel: !0, showGrid: !0, labelInterpolationFnc: t.noop, type: void 0, scaleMinSpace: 20, onlyInteger: !1 }, width: void 0, height: void 0, showLine: !0, showPoint: !0, showArea: !1, areaBase: 0, lineSmooth: !0, showGridBackground: !1, low: void 0, high: void 0, chartPadding: { top: 15, right: 15, bottom: 5, left: 10 }, fullWidth: !1, reverseData: !1, classNames: { chart: "ct-chart-line", label: "ct-label", labelGroup: "ct-labels", series: "ct-series", line: "ct-line", point: "ct-point", area: "ct-area", grid: "ct-grid", gridGroup: "ct-grids", gridBackground: "ct-grid-background", vertical: "ct-vertical", horizontal: "ct-horizontal", start: "ct-start", end: "ct-end" } }; t.Line = t.Base.extend({ constructor: function (e, n, s, r) { t.Line.super.constructor.call(this, e, n, i, t.extend({}, i, s), r) }, createChart: function (e) { var n = t.normalizeData(this.data, e.reverseData, !0); this.svg = t.createSvg(this.container, e.width, e.height, e.classNames.chart); var s, r, a = this.svg.elem("g").addClass(e.classNames.gridGroup), o = this.svg.elem("g"), l = this.svg.elem("g").addClass(e.classNames.labelGroup), h = t.createChartRect(this.svg, e, i.padding); s = void 0 === e.axisX.type ? new t.StepAxis(t.Axis.units.x, n.normalized.series, h, t.extend({}, e.axisX, { ticks: n.normalized.labels, stretch: e.fullWidth })) : e.axisX.type.call(t, t.Axis.units.x, n.normalized.series, h, e.axisX), r = void 0 === e.axisY.type ? new t.AutoScaleAxis(t.Axis.units.y, n.normalized.series, h, t.extend({}, e.axisY, { high: t.isNumeric(e.high) ? e.high : e.axisY.high, low: t.isNumeric(e.low) ? e.low : e.axisY.low })) : e.axisY.type.call(t, t.Axis.units.y, n.normalized.series, h, e.axisY), s.createGridAndLabels(a, l, this.supportsForeignObject, e, this.eventEmitter), r.createGridAndLabels(a, l, this.supportsForeignObject, e, this.eventEmitter), e.showGridBackground && t.createGridBackground(a, h, e.classNames.gridBackground, this.eventEmitter), n.raw.series.forEach(function (i, a) { var l = o.elem("g"); l.attr({ "ct:series-name": i.name, "ct:meta": t.serialize(i.meta) }), l.addClass([e.classNames.series, i.className || e.classNames.series + "-" + t.alphaNumerate(a)].join(" ")); var u = [], c = []; n.normalized.series[a].forEach(function (e, o) { var l = { x: h.x1 + s.projectValue(e, o, n.normalized.series[a]), y: h.y1 - r.projectValue(e, o, n.normalized.series[a]) }; u.push(l.x, l.y), c.push({ value: e, valueIndex: o, meta: t.getMetaData(i, o) }) }.bind(this)); var d = { lineSmooth: t.getSeriesOption(i, e, "lineSmooth"), showPoint: t.getSeriesOption(i, e, "showPoint"), showLine: t.getSeriesOption(i, e, "showLine"), showArea: t.getSeriesOption(i, e, "showArea"), areaBase: t.getSeriesOption(i, e, "areaBase") }, p = ("function" == typeof d.lineSmooth ? d.lineSmooth : d.lineSmooth ? t.Interpolation.monotoneCubic() : t.Interpolation.none())(u, c); if (d.showPoint && p.pathElements.forEach(function (n) { var o = l.elem("line", { x1: n.x, y1: n.y, x2: n.x + .01, y2: n.y }, e.classNames.point).attr({ "ct:value": [n.data.value.x, n.data.value.y].filter(t.isNumeric).join(","), "ct:meta": t.serialize(n.data.meta) }); this.eventEmitter.emit("draw", { type: "point", value: n.data.value, index: n.data.valueIndex, meta: n.data.meta, series: i, seriesIndex: a, axisX: s, axisY: r, group: l, element: o, x: n.x, y: n.y }) }.bind(this)), d.showLine) { var f = l.elem("path", { d: p.stringify() }, e.classNames.line, !0); this.eventEmitter.emit("draw", { type: "line", values: n.normalized.series[a], path: p.clone(), chartRect: h, index: a, series: i, seriesIndex: a, seriesMeta: i.meta, axisX: s, axisY: r, group: l, element: f }) } if (d.showArea && r.range) { var m = Math.max(Math.min(d.areaBase, r.range.max), r.range.min), g = h.y1 - r.projectValue(m); p.splitByCommand("M").filter((function (e) { return e.pathElements.length > 1 })).map((function (e) { var t = e.pathElements[0], i = e.pathElements[e.pathElements.length - 1]; return e.clone(!0).position(0).remove(1).move(t.x, g).line(t.x, t.y).position(e.pathElements.length + 1).line(i.x, g) })).forEach(function (t) { var o = l.elem("path", { d: t.stringify() }, e.classNames.area, !0); this.eventEmitter.emit("draw", { type: "area", values: n.normalized.series[a], path: t.clone(), series: i, seriesIndex: a, axisX: s, axisY: r, chartRect: h, index: a, group: l, element: o }) }.bind(this)) } }.bind(this)), this.eventEmitter.emit("created", { bounds: r.bounds, chartRect: h, axisX: s, axisY: r, svg: this.svg, options: e }) } }) }(this || global, e), function (e, t) { "use strict"; e.window, e.document; var i = { axisX: { offset: 30, position: "end", labelOffset: { x: 0, y: 0 }, showLabel: !0, showGrid: !0, labelInterpolationFnc: t.noop, scaleMinSpace: 30, onlyInteger: !1 }, axisY: { offset: 40, position: "start", labelOffset: { x: 0, y: 0 }, showLabel: !0, showGrid: !0, labelInterpolationFnc: t.noop, scaleMinSpace: 20, onlyInteger: !1 }, width: void 0, height: void 0, high: void 0, low: void 0, referenceValue: 0, chartPadding: { top: 15, right: 15, bottom: 5, left: 10 }, seriesBarDistance: 15, stackBars: !1, stackMode: "accumulate", horizontalBars: !1, distributeSeries: !1, reverseData: !1, showGridBackground: !1, classNames: { chart: "ct-chart-bar", horizontalBars: "ct-horizontal-bars", label: "ct-label", labelGroup: "ct-labels", series: "ct-series", bar: "ct-bar", grid: "ct-grid", gridGroup: "ct-grids", gridBackground: "ct-grid-background", vertical: "ct-vertical", horizontal: "ct-horizontal", start: "ct-start", end: "ct-end" } }; t.Bar = t.Base.extend({ constructor: function (e, n, s, r) { t.Bar.super.constructor.call(this, e, n, i, t.extend({}, i, s), r) }, createChart: function (e) { var n, s; e.distributeSeries ? (n = t.normalizeData(this.data, e.reverseData, e.horizontalBars ? "x" : "y")).normalized.series = n.normalized.series.map((function (e) { return [e] })) : n = t.normalizeData(this.data, e.reverseData, e.horizontalBars ? "x" : "y"), this.svg = t.createSvg(this.container, e.width, e.height, e.classNames.chart + (e.horizontalBars ? " " + e.classNames.horizontalBars : "")); var r = this.svg.elem("g").addClass(e.classNames.gridGroup), a = this.svg.elem("g"), o = this.svg.elem("g").addClass(e.classNames.labelGroup); if (e.stackBars && 0 !== n.normalized.series.length) { var l = t.serialMap(n.normalized.series, (function () { return Array.prototype.slice.call(arguments).map((function (e) { return e })).reduce((function (e, t) { return { x: e.x + (t && t.x) || 0, y: e.y + (t && t.y) || 0 } }), { x: 0, y: 0 }) })); s = t.getHighLow([l], e, e.horizontalBars ? "x" : "y") } else s = t.getHighLow(n.normalized.series, e, e.horizontalBars ? "x" : "y"); s.high = +e.high || (0 === e.high ? 0 : s.high), s.low = +e.low || (0 === e.low ? 0 : s.low); var h, u, c, d, p, f = t.createChartRect(this.svg, e, i.padding); u = e.distributeSeries && e.stackBars ? n.normalized.labels.slice(0, 1) : n.normalized.labels, e.horizontalBars ? (h = d = void 0 === e.axisX.type ? new t.AutoScaleAxis(t.Axis.units.x, n.normalized.series, f, t.extend({}, e.axisX, { highLow: s, referenceValue: 0 })) : e.axisX.type.call(t, t.Axis.units.x, n.normalized.series, f, t.extend({}, e.axisX, { highLow: s, referenceValue: 0 })), c = p = void 0 === e.axisY.type ? new t.StepAxis(t.Axis.units.y, n.normalized.series, f, { ticks: u }) : e.axisY.type.call(t, t.Axis.units.y, n.normalized.series, f, e.axisY)) : (c = d = void 0 === e.axisX.type ? new t.StepAxis(t.Axis.units.x, n.normalized.series, f, { ticks: u }) : e.axisX.type.call(t, t.Axis.units.x, n.normalized.series, f, e.axisX), h = p = void 0 === e.axisY.type ? new t.AutoScaleAxis(t.Axis.units.y, n.normalized.series, f, t.extend({}, e.axisY, { highLow: s, referenceValue: 0 })) : e.axisY.type.call(t, t.Axis.units.y, n.normalized.series, f, t.extend({}, e.axisY, { highLow: s, referenceValue: 0 }))); var m = e.horizontalBars ? f.x1 + h.projectValue(0) : f.y1 - h.projectValue(0), g = []; c.createGridAndLabels(r, o, this.supportsForeignObject, e, this.eventEmitter), h.createGridAndLabels(r, o, this.supportsForeignObject, e, this.eventEmitter), e.showGridBackground && t.createGridBackground(r, f, e.classNames.gridBackground, this.eventEmitter), n.raw.series.forEach(function (i, s) { var r, o, l = s - (n.raw.series.length - 1) / 2; r = e.distributeSeries && !e.stackBars ? c.axisLength / n.normalized.series.length / 2 : e.distributeSeries && e.stackBars ? c.axisLength / 2 : c.axisLength / n.normalized.series[s].length / 2, (o = a.elem("g")).attr({ "ct:series-name": i.name, "ct:meta": t.serialize(i.meta) }), o.addClass([e.classNames.series, i.className || e.classNames.series + "-" + t.alphaNumerate(s)].join(" ")), n.normalized.series[s].forEach(function (a, u) { var v, x, y, b; if (b = e.distributeSeries && !e.stackBars ? s : e.distributeSeries && e.stackBars ? 0 : u, v = e.horizontalBars ? { x: f.x1 + h.projectValue(a && a.x ? a.x : 0, u, n.normalized.series[s]), y: f.y1 - c.projectValue(a && a.y ? a.y : 0, b, n.normalized.series[s]) } : { x: f.x1 + c.projectValue(a && a.x ? a.x : 0, b, n.normalized.series[s]), y: f.y1 - h.projectValue(a && a.y ? a.y : 0, u, n.normalized.series[s]) }, c instanceof t.StepAxis && (c.options.stretch || (v[c.units.pos] += r * (e.horizontalBars ? -1 : 1)), v[c.units.pos] += e.stackBars || e.distributeSeries ? 0 : l * e.seriesBarDistance * (e.horizontalBars ? -1 : 1)), y = g[u] || m, g[u] = y - (m - v[c.counterUnits.pos]), void 0 !== a) { var w = {}; w[c.units.pos + "1"] = v[c.units.pos], w[c.units.pos + "2"] = v[c.units.pos], !e.stackBars || "accumulate" !== e.stackMode && e.stackMode ? (w[c.counterUnits.pos + "1"] = m, w[c.counterUnits.pos + "2"] = v[c.counterUnits.pos]) : (w[c.counterUnits.pos + "1"] = y, w[c.counterUnits.pos + "2"] = g[u]), w.x1 = Math.min(Math.max(w.x1, f.x1), f.x2), w.x2 = Math.min(Math.max(w.x2, f.x1), f.x2), w.y1 = Math.min(Math.max(w.y1, f.y2), f.y1), w.y2 = Math.min(Math.max(w.y2, f.y2), f.y1); var E = t.getMetaData(i, u); x = o.elem("line", w, e.classNames.bar).attr({ "ct:value": [a.x, a.y].filter(t.isNumeric).join(","), "ct:meta": t.serialize(E) }), this.eventEmitter.emit("draw", t.extend({ type: "bar", value: a, index: u, meta: E, series: i, seriesIndex: s, axisX: d, axisY: p, chartRect: f, group: o, element: x }, w)) } }.bind(this)) }.bind(this)), this.eventEmitter.emit("created", { bounds: h.bounds, chartRect: f, axisX: d, axisY: p, svg: this.svg, options: e }) } }) }(this || global, e), function (e, t) { "use strict"; e.window, e.document; var i = { width: void 0, height: void 0, chartPadding: 5, classNames: { chartPie: "ct-chart-pie", chartDonut: "ct-chart-donut", series: "ct-series", slicePie: "ct-slice-pie", sliceDonut: "ct-slice-donut", sliceDonutSolid: "ct-slice-donut-solid", label: "ct-label" }, startAngle: 0, total: void 0, donut: !1, donutSolid: !1, donutWidth: 60, showLabel: !0, labelOffset: 0, labelPosition: "inside", labelInterpolationFnc: t.noop, labelDirection: "neutral", reverseData: !1, ignoreEmptyValues: !1 }; function n(e, t, i) { var n = t.x > e.x; return n && "explode" === i || !n && "implode" === i ? "start" : n && "implode" === i || !n && "explode" === i ? "end" : "middle" } t.Pie = t.Base.extend({ constructor: function (e, n, s, r) { t.Pie.super.constructor.call(this, e, n, i, t.extend({}, i, s), r) }, createChart: function (e) { var s, r, a, o, l, h = t.normalizeData(this.data), u = [], c = e.startAngle; this.svg = t.createSvg(this.container, e.width, e.height, e.donut ? e.classNames.chartDonut : e.classNames.chartPie), r = t.createChartRect(this.svg, e, i.padding), a = Math.min(r.width() / 2, r.height() / 2), l = e.total || h.normalized.series.reduce((function (e, t) { return e + t }), 0); var d = t.quantity(e.donutWidth); "%" === d.unit && (d.value *= a / 100), a -= e.donut && !e.donutSolid ? d.value / 2 : 0, o = "outside" === e.labelPosition || e.donut && !e.donutSolid ? a : "center" === e.labelPosition ? 0 : e.donutSolid ? a - d.value / 2 : a / 2, o += e.labelOffset; var p = { x: r.x1 + r.width() / 2, y: r.y2 + r.height() / 2 }, f = 1 === h.raw.series.filter((function (e) { return e.hasOwnProperty("value") ? 0 !== e.value : 0 !== e })).length; h.raw.series.forEach(function (e, t) { u[t] = this.svg.elem("g", null, null) }.bind(this)), e.showLabel && (s = this.svg.elem("g", null, null)), h.raw.series.forEach(function (i, r) { if (0 !== h.normalized.series[r] || !e.ignoreEmptyValues) { u[r].attr({ "ct:series-name": i.name }), u[r].addClass([e.classNames.series, i.className || e.classNames.series + "-" + t.alphaNumerate(r)].join(" ")); var m = l > 0 ? c + h.normalized.series[r] / l * 360 : 0, g = Math.max(0, c - (0 === r || f ? 0 : .2)); m - g >= 359.99 && (m = g + 359.99); var v, x, y, b = t.polarToCartesian(p.x, p.y, a, g), w = t.polarToCartesian(p.x, p.y, a, m), E = new t.Svg.Path(!e.donut || e.donutSolid).move(w.x, w.y).arc(a, a, 0, m - c > 180, 0, b.x, b.y); e.donut ? e.donutSolid && (y = a - d.value, v = t.polarToCartesian(p.x, p.y, y, c - (0 === r || f ? 0 : .2)), x = t.polarToCartesian(p.x, p.y, y, m), E.line(v.x, v.y), E.arc(y, y, 0, m - c > 180, 1, x.x, x.y)) : E.line(p.x, p.y); var S = e.classNames.slicePie; e.donut && (S = e.classNames.sliceDonut, e.donutSolid && (S = e.classNames.sliceDonutSolid)); var A = u[r].elem("path", { d: E.stringify() }, S); if (A.attr({ "ct:value": h.normalized.series[r], "ct:meta": t.serialize(i.meta) }), e.donut && !e.donutSolid && (A._node.style.strokeWidth = d.value + "px"), this.eventEmitter.emit("draw", { type: "slice", value: h.normalized.series[r], totalDataSum: l, index: r, meta: i.meta, series: i, group: u[r], element: A, path: E.clone(), center: p, radius: a, startAngle: c, endAngle: m }), e.showLabel) { var z, M; z = 1 === h.raw.series.length ? { x: p.x, y: p.y } : t.polarToCartesian(p.x, p.y, o, c + (m - c) / 2), M = h.normalized.labels && !t.isFalseyButZero(h.normalized.labels[r]) ? h.normalized.labels[r] : h.normalized.series[r]; var O = e.labelInterpolationFnc(M, r); if (O || 0 === O) { var C = s.elem("text", { dx: z.x, dy: z.y, "text-anchor": n(p, z, e.labelDirection) }, e.classNames.label).text("" + O); this.eventEmitter.emit("draw", { type: "label", index: r, group: s, element: C, text: "" + O, x: z.x, y: z.y }) } } c = m } }.bind(this)), this.eventEmitter.emit("created", { chartRect: r, svg: this.svg, options: e }) }, determineAnchorPosition: n }) }(this || global, e), e }));

var i, l, selectedLine = null;

/* Navigate to hash without browser history entry */
var navigateToHash = function () {
    if (window.history !== undefined && window.history.replaceState !== undefined) {
        window.history.replaceState(undefined, undefined, this.getAttribute("href"));
    }
};

var hashLinks = document.getElementsByClassName('navigatetohash');
for (i = 0, l = hashLinks.length; i < l; i++) {
    hashLinks[i].addEventListener('click', navigateToHash);
}

/* Switch test method */
var switchTestMethod = function () {
    var method = this.getAttribute("value");
    console.log("Selected test method: " + method);

    var lines, i, l, coverageData, lineAnalysis, cells;

    lines = document.querySelectorAll('.lineAnalysis tr');

    for (i = 1, l = lines.length; i < l; i++) {
        coverageData = JSON.parse(lines[i].getAttribute('data-coverage').replace(/'/g, '"'));
        lineAnalysis = coverageData[method];
        cells = lines[i].querySelectorAll('td');
        if (lineAnalysis === undefined) {
            lineAnalysis = coverageData.AllTestMethods;
            if (lineAnalysis.LVS !== 'gray') {
                cells[0].setAttribute('class', 'red');
                cells[1].innerText = cells[1].textContent = '0';
                cells[4].setAttribute('class', 'lightred');
            }
        } else {
            cells[0].setAttribute('class', lineAnalysis.LVS);
            cells[1].innerText = cells[1].textContent = lineAnalysis.VC;
            cells[4].setAttribute('class', 'light' + lineAnalysis.LVS);
        }
    }
};

var testMethods = document.getElementsByClassName('switchtestmethod');
for (i = 0, l = testMethods.length; i < l; i++) {
    testMethods[i].addEventListener('change', switchTestMethod);
}

/* Highlight test method by line */
var toggleLine = function () {
    if (selectedLine === this) {
        selectedLine = null;
    } else {
        selectedLine = null;
        unhighlightTestMethods();
        highlightTestMethods.call(this);
        selectedLine = this;
    }
    
};
var highlightTestMethods = function () {
    if (selectedLine !== null) {
        return;
    }

    var lineAnalysis;
    var coverageData = JSON.parse(this.getAttribute('data-coverage').replace(/'/g, '"'));
    var testMethods = document.getElementsByClassName('testmethod');

    for (i = 0, l = testMethods.length; i < l; i++) {
        lineAnalysis = coverageData[testMethods[i].id];
        if (lineAnalysis === undefined) {
            testMethods[i].className = testMethods[i].className.replace(/\s*light.+/g, "");
        } else {
            testMethods[i].className += ' light' + lineAnalysis.LVS;
        }
    }
};
var unhighlightTestMethods = function () {
    if (selectedLine !== null) {
        return;
    }

    var testMethods = document.getElementsByClassName('testmethod');
    for (i = 0, l = testMethods.length; i < l; i++) {
        testMethods[i].className = testMethods[i].className.replace(/\s*light.+/g, "");
    }
};
var coverableLines = document.getElementsByClassName('coverableline');
for (i = 0, l = coverableLines.length; i < l; i++) {
    coverableLines[i].addEventListener('click', toggleLine);
    coverableLines[i].addEventListener('mouseenter', highlightTestMethods);
    coverableLines[i].addEventListener('mouseleave', unhighlightTestMethods);
}

/* History charts */
var renderChart = function (chart) {
    // Remove current children (e.g. PNG placeholder)
    while (chart.firstChild) {
        chart.firstChild.remove();
    }

    var chartData = window[chart.getAttribute('data-data')];
    var options = {
        axisY: {
            type: undefined,
            onlyInteger: true
        },
        lineSmooth: false,
        low: 0,
        high: 100,
        scaleMinSpace: 20,
        onlyInteger: true,
        fullWidth: true
    };
    var lineChart = new Chartist.Line(chart, {
        labels: [],
        series: chartData.series
    }, options);

    /* Zoom */
    var zoomButtonDiv = document.createElement("div");
    zoomButtonDiv.className = "toggleZoom";
    var zoomButtonLink = document.createElement("a");
    zoomButtonLink.setAttribute("href", "");
    var zoomButtonText = document.createElement("i");
    zoomButtonText.className = "icon-search-plus";

    zoomButtonLink.appendChild(zoomButtonText);
    zoomButtonDiv.appendChild(zoomButtonLink);

    chart.appendChild(zoomButtonDiv);

    zoomButtonDiv.addEventListener('click', function (event) {
        event.preventDefault();

        if (options.axisY.type === undefined) {
            options.axisY.type = Chartist.AutoScaleAxis;
            zoomButtonText.className = "icon-search-minus";
        } else {
            options.axisY.type = undefined;
            zoomButtonText.className = "icon-search-plus";
        }

        lineChart.update(null, options);
    });

    var tooltip = document.createElement("div");
    tooltip.className = "tooltip";

    chart.appendChild(tooltip);

    /* Tooltips */
    var showToolTip = function () {
        var index = this.getAttribute('ct:meta');

        tooltip.innerHTML = chartData.tooltips[index];
        tooltip.style.display = 'block';
    };

    var moveToolTip = function (event) {
        var box = chart.getBoundingClientRect();
        var left = event.pageX - box.left - window.pageXOffset;
        var top = event.pageY - box.top - window.pageYOffset;

        left = left + 20;
        top = top - tooltip.offsetHeight / 2;

        if (left + tooltip.offsetWidth > box.width) {
            left -= tooltip.offsetWidth + 40;
        }

        if (top < 0) {
            top = 0;
        }

        if (top + tooltip.offsetHeight > box.height) {
            top = box.height - tooltip.offsetHeight;
        }

        tooltip.style.left = left + 'px';
        tooltip.style.top = top + 'px';
    };

    var hideToolTip = function () {
        tooltip.style.display = 'none';
    };
    chart.addEventListener('mousemove', moveToolTip);

    lineChart.on('created', function () {
        var chartPoints = chart.getElementsByClassName('ct-point');
        for (i = 0, l = chartPoints.length; i < l; i++) {
            chartPoints[i].addEventListener('mousemove', showToolTip);
            chartPoints[i].addEventListener('mouseout', hideToolTip);
        }
    });
};

var charts = document.getElementsByClassName('historychart');
for (i = 0, l = charts.length; i < l; i++) {
    renderChart(charts[i]);
}

/* History diff charts */
var renderDiffChart = function (chart) {
    // Remove current children (e.g. PNG placeholder)
    while (chart.firstChild) {
        chart.firstChild.remove();
    }

    var chartData = window[chart.getAttribute('data-data')];
    var options = {
        low: -chartData.minMaxValue,
        high: chartData.minMaxValue,
        axisY: {
            onlyInteger: true
        }
    };
    var barChart = new Chartist.Bar(chart, {
        series: chartData.series
    }, options);

    var tooltip = document.createElement("div");
    tooltip.className = "tooltip";
    tooltip.innerHTML = chartData.tooltip;

    chart.appendChild(tooltip);

    /* Tooltips */
    /* Tooltips */
    var showToolTip = function () {
        var index = this.getAttribute('ct:meta');

        tooltip.innerHTML = chartData.tooltips[index];
        tooltip.style.display = 'block';
    };

    var moveToolTip = function (event) {
        var box = chart.getBoundingClientRect();
        var left = event.pageX - box.left - window.pageXOffset;
        var top = event.pageY - box.top - window.pageYOffset;

        left = left + 20;
        top = top - tooltip.offsetHeight / 2;

        if (left + tooltip.offsetWidth > box.width) {
            left -= tooltip.offsetWidth + 40;
        }

        if (top < 0) {
            top = 0;
        }

        if (top + tooltip.offsetHeight > box.height) {
            top = box.height - tooltip.offsetHeight;
        }

        tooltip.style.left = left + 'px';
        tooltip.style.top = top + 'px';
    };

    var hideToolTip = function () {
        tooltip.style.display = 'none';
    };
    chart.addEventListener('mousemove', moveToolTip);

    barChart.on('created', function () {
        var chartBars = chart.getElementsByClassName('ct-bar');
        for (i = 0, l = chartBars.length; i < l; i++) {
            chartBars[i].addEventListener('mousemove', showToolTip);
            chartBars[i].addEventListener('mouseout', hideToolTip);
        }
    });
};

var charts = document.getElementsByClassName('historydiffchart');
for (i = 0, l = charts.length; i < l; i++) {
    renderDiffChart(charts[i]);
}