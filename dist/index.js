import { createRequire } from "node:module";
var __defProp = Object.defineProperty;
var __returnValue = (v) => v;
function __exportSetter(name, newValue) {
  this[name] = __returnValue.bind(null, newValue);
}
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, {
      get: all[name],
      enumerable: true,
      configurable: true,
      set: __exportSetter.bind(all, name)
    });
};
var __esm = (fn, res) => () => (fn && (res = fn(fn = 0)), res);
var __require = /* @__PURE__ */ createRequire(import.meta.url);

// node_modules/fzstd/esm/index.mjs
function decompress(dat, buf) {
  var bufs = [], nb = +!buf;
  var bt = 0, ol = 0;
  for (;dat.length; ) {
    var st = rzfh(dat, nb || buf);
    if (typeof st == "object") {
      if (nb) {
        buf = null;
        if (st.w.length == st.u) {
          bufs.push(buf = st.w);
          ol += st.u;
        }
      } else {
        bufs.push(buf);
        st.e = 0;
      }
      for (;!st.l; ) {
        var blk = rzb(dat, st, buf);
        if (!blk)
          err(5);
        if (buf)
          st.e = st.y;
        else {
          bufs.push(blk);
          ol += blk.length;
          cpw(st.w, 0, blk.length);
          st.w.set(blk, st.w.length - blk.length);
        }
      }
      bt = st.b + st.c * 4;
    } else
      bt = st;
    dat = dat.subarray(bt);
  }
  return cct(bufs, ol);
}
var ab, u8, u16, i16, i32, slc = function(v, s, e) {
  if (u8.prototype.slice)
    return u8.prototype.slice.call(v, s, e);
  if (s == null || s < 0)
    s = 0;
  if (e == null || e > v.length)
    e = v.length;
  var n = new u8(e - s);
  n.set(v.subarray(s, e));
  return n;
}, fill = function(v, n, s, e) {
  if (u8.prototype.fill)
    return u8.prototype.fill.call(v, n, s, e);
  if (s == null || s < 0)
    s = 0;
  if (e == null || e > v.length)
    e = v.length;
  for (;s < e; ++s)
    v[s] = n;
  return v;
}, cpw = function(v, t, s, e) {
  if (u8.prototype.copyWithin)
    return u8.prototype.copyWithin.call(v, t, s, e);
  if (s == null || s < 0)
    s = 0;
  if (e == null || e > v.length)
    e = v.length;
  while (s < e) {
    v[t++] = v[s++];
  }
}, ec, err = function(ind, msg, nt) {
  var e = new Error(msg || ec[ind]);
  e.code = ind;
  if (Error.captureStackTrace)
    Error.captureStackTrace(e, err);
  if (!nt)
    throw e;
  return e;
}, rb = function(d, b, n) {
  var i = 0, o = 0;
  for (;i < n; ++i)
    o |= d[b++] << (i << 3);
  return o;
}, b4 = function(d, b) {
  return (d[b] | d[b + 1] << 8 | d[b + 2] << 16 | d[b + 3] << 24) >>> 0;
}, rzfh = function(dat, w) {
  var n3 = dat[0] | dat[1] << 8 | dat[2] << 16;
  if (n3 == 3126568 && dat[3] == 253) {
    var flg = dat[4];
    var ss = flg >> 5 & 1, cc = flg >> 2 & 1, df = flg & 3, fcf = flg >> 6;
    if (flg & 8)
      err(0);
    var bt = 6 - ss;
    var db = df == 3 ? 4 : df;
    var di = rb(dat, bt, db);
    bt += db;
    var fsb = fcf ? 1 << fcf : ss;
    var fss = rb(dat, bt, fsb) + (fcf == 1 && 256);
    var ws = fss;
    if (!ss) {
      var wb = 1 << 10 + (dat[5] >> 3);
      ws = wb + (wb >> 3) * (dat[5] & 7);
    }
    if (ws > 2145386496)
      err(1);
    var buf = new u8((w == 1 ? fss || ws : w ? 0 : ws) + 12);
    buf[0] = 1, buf[4] = 4, buf[8] = 8;
    return {
      b: bt + fsb,
      y: 0,
      l: 0,
      d: di,
      w: w && w != 1 ? w : buf.subarray(12),
      e: ws,
      o: new i32(buf.buffer, 0, 3),
      u: fss,
      c: cc,
      m: Math.min(131072, ws)
    };
  } else if ((n3 >> 4 | dat[3] << 20) == 25481893) {
    return b4(dat, 4) + 8;
  }
  err(0);
}, msb = function(val) {
  var bits = 0;
  for (;1 << bits <= val; ++bits)
    ;
  return bits - 1;
}, rfse = function(dat, bt, mal) {
  var tpos = (bt << 3) + 4;
  var al = (dat[bt] & 15) + 5;
  if (al > mal)
    err(3);
  var sz = 1 << al;
  var probs = sz, sym = -1, re = -1, i = -1, ht = sz;
  var buf = new ab(512 + (sz << 2));
  var freq = new i16(buf, 0, 256);
  var dstate = new u16(buf, 0, 256);
  var nstate = new u16(buf, 512, sz);
  var bb1 = 512 + (sz << 1);
  var syms = new u8(buf, bb1, sz);
  var nbits = new u8(buf, bb1 + sz);
  while (sym < 255 && probs > 0) {
    var bits = msb(probs + 1);
    var cbt = tpos >> 3;
    var msk = (1 << bits + 1) - 1;
    var val = (dat[cbt] | dat[cbt + 1] << 8 | dat[cbt + 2] << 16) >> (tpos & 7) & msk;
    var msk1fb = (1 << bits) - 1;
    var msv = msk - probs - 1;
    var sval = val & msk1fb;
    if (sval < msv)
      tpos += bits, val = sval;
    else {
      tpos += bits + 1;
      if (val > msk1fb)
        val -= msv;
    }
    freq[++sym] = --val;
    if (val == -1) {
      probs += val;
      syms[--ht] = sym;
    } else
      probs -= val;
    if (!val) {
      do {
        var rbt = tpos >> 3;
        re = (dat[rbt] | dat[rbt + 1] << 8) >> (tpos & 7) & 3;
        tpos += 2;
        sym += re;
      } while (re == 3);
    }
  }
  if (sym > 255 || probs)
    err(0);
  var sympos = 0;
  var sstep = (sz >> 1) + (sz >> 3) + 3;
  var smask = sz - 1;
  for (var s = 0;s <= sym; ++s) {
    var sf = freq[s];
    if (sf < 1) {
      dstate[s] = -sf;
      continue;
    }
    for (i = 0;i < sf; ++i) {
      syms[sympos] = s;
      do {
        sympos = sympos + sstep & smask;
      } while (sympos >= ht);
    }
  }
  if (sympos)
    err(0);
  for (i = 0;i < sz; ++i) {
    var ns = dstate[syms[i]]++;
    var nb = nbits[i] = al - msb(ns);
    nstate[i] = (ns << nb) - sz;
  }
  return [tpos + 7 >> 3, {
    b: al,
    s: syms,
    n: nbits,
    t: nstate
  }];
}, rhu = function(dat, bt) {
  var i = 0, wc = -1;
  var buf = new u8(292), hb = dat[bt];
  var hw = buf.subarray(0, 256);
  var rc = buf.subarray(256, 268);
  var ri = new u16(buf.buffer, 268);
  if (hb < 128) {
    var _a = rfse(dat, bt + 1, 6), ebt = _a[0], fdt = _a[1];
    bt += hb;
    var epos = ebt << 3;
    var lb = dat[bt];
    if (!lb)
      err(0);
    var st1 = 0, st2 = 0, btr1 = fdt.b, btr2 = btr1;
    var fpos = (++bt << 3) - 8 + msb(lb);
    for (;; ) {
      fpos -= btr1;
      if (fpos < epos)
        break;
      var cbt = fpos >> 3;
      st1 += (dat[cbt] | dat[cbt + 1] << 8) >> (fpos & 7) & (1 << btr1) - 1;
      hw[++wc] = fdt.s[st1];
      fpos -= btr2;
      if (fpos < epos)
        break;
      cbt = fpos >> 3;
      st2 += (dat[cbt] | dat[cbt + 1] << 8) >> (fpos & 7) & (1 << btr2) - 1;
      hw[++wc] = fdt.s[st2];
      btr1 = fdt.n[st1];
      st1 = fdt.t[st1];
      btr2 = fdt.n[st2];
      st2 = fdt.t[st2];
    }
    if (++wc > 255)
      err(0);
  } else {
    wc = hb - 127;
    for (;i < wc; i += 2) {
      var byte = dat[++bt];
      hw[i] = byte >> 4;
      hw[i + 1] = byte & 15;
    }
    ++bt;
  }
  var wes = 0;
  for (i = 0;i < wc; ++i) {
    var wt = hw[i];
    if (wt > 11)
      err(0);
    wes += wt && 1 << wt - 1;
  }
  var mb = msb(wes) + 1;
  var ts = 1 << mb;
  var rem = ts - wes;
  if (rem & rem - 1)
    err(0);
  hw[wc++] = msb(rem) + 1;
  for (i = 0;i < wc; ++i) {
    var wt = hw[i];
    ++rc[hw[i] = wt && mb + 1 - wt];
  }
  var hbuf = new u8(ts << 1);
  var syms = hbuf.subarray(0, ts), nb = hbuf.subarray(ts);
  ri[mb] = 0;
  for (i = mb;i > 0; --i) {
    var pv = ri[i];
    fill(nb, i, pv, ri[i - 1] = pv + rc[i] * (1 << mb - i));
  }
  if (ri[0] != ts)
    err(0);
  for (i = 0;i < wc; ++i) {
    var bits = hw[i];
    if (bits) {
      var code = ri[bits];
      fill(syms, i, code, ri[bits] = code + (1 << mb - bits));
    }
  }
  return [bt, {
    n: nb,
    b: mb,
    s: syms
  }];
}, dllt, dmlt, doct, b2bl = function(b, s) {
  var len = b.length, bl = new i32(len);
  for (var i = 0;i < len; ++i) {
    bl[i] = s;
    s += 1 << b[i];
  }
  return bl;
}, llb, llbl, mlb, mlbl, dhu = function(dat, out, hu) {
  var len = dat.length, ss = out.length, lb = dat[len - 1], msk = (1 << hu.b) - 1, eb = -hu.b;
  if (!lb)
    err(0);
  var st = 0, btr = hu.b, pos = (len << 3) - 8 + msb(lb) - btr, i = -1;
  for (;pos > eb && i < ss; ) {
    var cbt = pos >> 3;
    var val = (dat[cbt] | dat[cbt + 1] << 8 | dat[cbt + 2] << 16) >> (pos & 7);
    st = (st << btr | val) & msk;
    out[++i] = hu.s[st];
    pos -= btr = hu.n[st];
  }
  if (pos != eb || i + 1 != ss)
    err(0);
}, dhu4 = function(dat, out, hu) {
  var bt = 6;
  var ss = out.length, sz1 = ss + 3 >> 2, sz2 = sz1 << 1, sz3 = sz1 + sz2;
  dhu(dat.subarray(bt, bt += dat[0] | dat[1] << 8), out.subarray(0, sz1), hu);
  dhu(dat.subarray(bt, bt += dat[2] | dat[3] << 8), out.subarray(sz1, sz2), hu);
  dhu(dat.subarray(bt, bt += dat[4] | dat[5] << 8), out.subarray(sz2, sz3), hu);
  dhu(dat.subarray(bt), out.subarray(sz3), hu);
}, rzb = function(dat, st, out) {
  var _a;
  var bt = st.b;
  var b0 = dat[bt], btype = b0 >> 1 & 3;
  st.l = b0 & 1;
  var sz = b0 >> 3 | dat[bt + 1] << 5 | dat[bt + 2] << 13;
  var ebt = (bt += 3) + sz;
  if (btype == 1) {
    if (bt >= dat.length)
      return;
    st.b = bt + 1;
    if (out) {
      fill(out, dat[bt], st.y, st.y += sz);
      return out;
    }
    return fill(new u8(sz), dat[bt]);
  }
  if (ebt > dat.length)
    return;
  if (btype == 0) {
    st.b = ebt;
    if (out) {
      out.set(dat.subarray(bt, ebt), st.y);
      st.y += sz;
      return out;
    }
    return slc(dat, bt, ebt);
  }
  if (btype == 2) {
    var b3 = dat[bt], lbt = b3 & 3, sf = b3 >> 2 & 3;
    var lss = b3 >> 4, lcs = 0, s4 = 0;
    if (lbt < 2) {
      if (sf & 1)
        lss |= dat[++bt] << 4 | (sf & 2 && dat[++bt] << 12);
      else
        lss = b3 >> 3;
    } else {
      s4 = sf;
      if (sf < 2)
        lss |= (dat[++bt] & 63) << 4, lcs = dat[bt] >> 6 | dat[++bt] << 2;
      else if (sf == 2)
        lss |= dat[++bt] << 4 | (dat[++bt] & 3) << 12, lcs = dat[bt] >> 2 | dat[++bt] << 6;
      else
        lss |= dat[++bt] << 4 | (dat[++bt] & 63) << 12, lcs = dat[bt] >> 6 | dat[++bt] << 2 | dat[++bt] << 10;
    }
    ++bt;
    var buf = out ? out.subarray(st.y, st.y + st.m) : new u8(st.m);
    var spl = buf.length - lss;
    if (lbt == 0)
      buf.set(dat.subarray(bt, bt += lss), spl);
    else if (lbt == 1)
      fill(buf, dat[bt++], spl);
    else {
      var hu = st.h;
      if (lbt == 2) {
        var hud = rhu(dat, bt);
        lcs += bt - (bt = hud[0]);
        st.h = hu = hud[1];
      } else if (!hu)
        err(0);
      (s4 ? dhu4 : dhu)(dat.subarray(bt, bt += lcs), buf.subarray(spl), hu);
    }
    var ns = dat[bt++];
    if (ns) {
      if (ns == 255)
        ns = (dat[bt++] | dat[bt++] << 8) + 32512;
      else if (ns > 127)
        ns = ns - 128 << 8 | dat[bt++];
      var scm = dat[bt++];
      if (scm & 3)
        err(0);
      var dts = [dmlt, doct, dllt];
      for (var i = 2;i > -1; --i) {
        var md = scm >> (i << 1) + 2 & 3;
        if (md == 1) {
          var rbuf = new u8([0, 0, dat[bt++]]);
          dts[i] = {
            s: rbuf.subarray(2, 3),
            n: rbuf.subarray(0, 1),
            t: new u16(rbuf.buffer, 0, 1),
            b: 0
          };
        } else if (md == 2) {
          _a = rfse(dat, bt, 9 - (i & 1)), bt = _a[0], dts[i] = _a[1];
        } else if (md == 3) {
          if (!st.t)
            err(0);
          dts[i] = st.t[i];
        }
      }
      var _b = st.t = dts, mlt = _b[0], oct = _b[1], llt = _b[2];
      var lb = dat[ebt - 1];
      if (!lb)
        err(0);
      var spos = (ebt << 3) - 8 + msb(lb) - llt.b, cbt = spos >> 3, oubt = 0;
      var lst = (dat[cbt] | dat[cbt + 1] << 8) >> (spos & 7) & (1 << llt.b) - 1;
      cbt = (spos -= oct.b) >> 3;
      var ost = (dat[cbt] | dat[cbt + 1] << 8) >> (spos & 7) & (1 << oct.b) - 1;
      cbt = (spos -= mlt.b) >> 3;
      var mst = (dat[cbt] | dat[cbt + 1] << 8) >> (spos & 7) & (1 << mlt.b) - 1;
      for (++ns;--ns; ) {
        var llc = llt.s[lst];
        var lbtr = llt.n[lst];
        var mlc = mlt.s[mst];
        var mbtr = mlt.n[mst];
        var ofc = oct.s[ost];
        var obtr = oct.n[ost];
        cbt = (spos -= ofc) >> 3;
        var ofp = 1 << ofc;
        var off = ofp + ((dat[cbt] | dat[cbt + 1] << 8 | dat[cbt + 2] << 16 | dat[cbt + 3] << 24) >>> (spos & 7) & ofp - 1);
        cbt = (spos -= mlb[mlc]) >> 3;
        var ml = mlbl[mlc] + ((dat[cbt] | dat[cbt + 1] << 8 | dat[cbt + 2] << 16) >> (spos & 7) & (1 << mlb[mlc]) - 1);
        cbt = (spos -= llb[llc]) >> 3;
        var ll = llbl[llc] + ((dat[cbt] | dat[cbt + 1] << 8 | dat[cbt + 2] << 16) >> (spos & 7) & (1 << llb[llc]) - 1);
        cbt = (spos -= lbtr) >> 3;
        lst = llt.t[lst] + ((dat[cbt] | dat[cbt + 1] << 8) >> (spos & 7) & (1 << lbtr) - 1);
        cbt = (spos -= mbtr) >> 3;
        mst = mlt.t[mst] + ((dat[cbt] | dat[cbt + 1] << 8) >> (spos & 7) & (1 << mbtr) - 1);
        cbt = (spos -= obtr) >> 3;
        ost = oct.t[ost] + ((dat[cbt] | dat[cbt + 1] << 8) >> (spos & 7) & (1 << obtr) - 1);
        if (off > 3) {
          st.o[2] = st.o[1];
          st.o[1] = st.o[0];
          st.o[0] = off -= 3;
        } else {
          var idx = off - (ll != 0);
          if (idx) {
            off = idx == 3 ? st.o[0] - 1 : st.o[idx];
            if (idx > 1)
              st.o[2] = st.o[1];
            st.o[1] = st.o[0];
            st.o[0] = off;
          } else
            off = st.o[0];
        }
        for (var i = 0;i < ll; ++i) {
          buf[oubt + i] = buf[spl + i];
        }
        oubt += ll, spl += ll;
        var stin = oubt - off;
        if (stin < 0) {
          var len = -stin;
          var bs = st.e + stin;
          if (len > ml)
            len = ml;
          for (var i = 0;i < len; ++i) {
            buf[oubt + i] = st.w[bs + i];
          }
          oubt += len, ml -= len, stin = 0;
        }
        for (var i = 0;i < ml; ++i) {
          buf[oubt + i] = buf[stin + i];
        }
        oubt += ml;
      }
      if (oubt != spl) {
        while (spl < buf.length) {
          buf[oubt++] = buf[spl++];
        }
      } else
        oubt = buf.length;
      if (out)
        st.y += oubt;
      else
        buf = slc(buf, 0, oubt);
    } else if (out) {
      st.y += lss;
      if (spl) {
        for (var i = 0;i < lss; ++i) {
          buf[i] = buf[spl + i];
        }
      }
    } else if (spl)
      buf = slc(buf, spl);
    st.b = ebt;
    return buf;
  }
  err(2);
}, cct = function(bufs, ol) {
  if (bufs.length == 1)
    return bufs[0];
  var buf = new u8(ol);
  for (var i = 0, b = 0;i < bufs.length; ++i) {
    var chk = bufs[i];
    buf.set(chk, b);
    b += chk.length;
  }
  return buf;
};
var init_esm = __esm(() => {
  ab = ArrayBuffer;
  u8 = Uint8Array;
  u16 = Uint16Array;
  i16 = Int16Array;
  i32 = Int32Array;
  ec = [
    "invalid zstd data",
    "window size too large (>2046MB)",
    "invalid block type",
    "FSE accuracy too high",
    "match distance too far back",
    "unexpected EOF"
  ];
  dllt = rfse(/* @__PURE__ */ new u8([
    81,
    16,
    99,
    140,
    49,
    198,
    24,
    99,
    12,
    33,
    196,
    24,
    99,
    102,
    102,
    134,
    70,
    146,
    4
  ]), 0, 6)[1];
  dmlt = rfse(/* @__PURE__ */ new u8([
    33,
    20,
    196,
    24,
    99,
    140,
    33,
    132,
    16,
    66,
    8,
    33,
    132,
    16,
    66,
    8,
    33,
    68,
    68,
    68,
    68,
    68,
    68,
    68,
    68,
    36,
    9
  ]), 0, 6)[1];
  doct = rfse(/* @__PURE__ */ new u8([
    32,
    132,
    16,
    66,
    102,
    70,
    68,
    68,
    68,
    68,
    36,
    73,
    2
  ]), 0, 5)[1];
  llb = /* @__PURE__ */ new u8((/* @__PURE__ */ new i32([
    0,
    0,
    0,
    0,
    16843009,
    50528770,
    134678020,
    202050057,
    269422093
  ])).buffer, 0, 36);
  llbl = /* @__PURE__ */ b2bl(llb, 0);
  mlb = /* @__PURE__ */ new u8((/* @__PURE__ */ new i32([
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    16843009,
    50528770,
    117769220,
    185207048,
    252579084,
    16
  ])).buffer, 0, 53);
  mlbl = /* @__PURE__ */ b2bl(mlb, 3);
});

// src/util/zstd.ts
var exports_zstd = {};
__export(exports_zstd, {
  zstdDecompress: () => zstdDecompress,
  zstdCompress: () => zstdCompress,
  isZstdCompressAvailable: () => isZstdCompressAvailable
});
function _loadZlibOrNull() {
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req)
    return null;
  try {
    return req(_NODE_ZLIB_MOD);
  } catch {
    return null;
  }
}
function isZstdCompressAvailable() {
  if (isBun)
    return true;
  const zlib = _loadZlibOrNull();
  return typeof zlib?.zstdCompressSync === "function";
}
async function zstdCompress(data, level) {
  if (isBun) {
    return new Uint8Array(Bun.zstdCompressSync(data, { level }));
  }
  const zlib = _loadZlibOrNull();
  const fn = zlib?.zstdCompressSync;
  if (typeof fn !== "function") {
    throw new Error("zstd compression is not available in this runtime. " + "Requires Bun or Node.js >= 22.15 / Deno >= 2.6.9. " + "(workerd has no native zstd encoder; fzstd is decompress-only.)");
  }
  return new Uint8Array(fn(data, {
    params: {
      [zlib.constants.ZSTD_c_compressionLevel]: level
    }
  }));
}
async function zstdDecompress(data, maxOutputSize) {
  if (maxOutputSize != null) {
    const declared = readZstdFrameContentSize(data);
    if (declared !== null && declared > maxOutputSize) {
      throw new Error(`zstd decompressed size (${declared}) would exceed cap (${maxOutputSize})`);
    }
  }
  let out;
  if (isBun) {
    out = new Uint8Array(Bun.zstdDecompressSync(data));
  } else {
    const zlib = _loadZlibOrNull();
    const fn = zlib?.zstdDecompressSync;
    if (typeof fn === "function") {
      out = new Uint8Array(fn(data));
    } else {
      const decoded = decompress(data);
      out = new Uint8Array(decoded.byteLength);
      out.set(decoded);
    }
  }
  if (maxOutputSize != null && out.byteLength > maxOutputSize) {
    throw new Error(`zstd decompressed size (${out.byteLength}) exceeds cap (${maxOutputSize})`);
  }
  return out;
}
function readZstdFrameContentSize(data) {
  if (data.length < 6)
    return null;
  if (data[0] !== 40 || data[1] !== 181 || data[2] !== 47 || data[3] !== 253) {
    return null;
  }
  const fhd = data[4];
  const fcsFieldSize = fhd >> 6 & 3;
  const singleSegment = (fhd >> 5 & 1) === 1;
  const dictIdFlag = fhd & 3;
  const fcsSize = fcsFieldSize === 0 ? singleSegment ? 1 : 0 : fcsFieldSize === 1 ? 2 : fcsFieldSize === 2 ? 4 : 8;
  if (fcsSize === 0)
    return null;
  const windowDescSize = singleSegment ? 0 : 1;
  const dictIdSize = dictIdFlag === 0 ? 0 : dictIdFlag === 1 ? 1 : dictIdFlag === 2 ? 2 : 4;
  const fcsOffset = 5 + windowDescSize + dictIdSize;
  if (data.length < fcsOffset + fcsSize)
    return null;
  let fcs = 0n;
  for (let i = 0;i < fcsSize; i++) {
    fcs |= BigInt(data[fcsOffset + i]) << BigInt(i * 8);
  }
  if (fcsSize === 2)
    fcs += 256n;
  if (fcs > BigInt(Number.MAX_SAFE_INTEGER))
    return Number.MAX_SAFE_INTEGER;
  return Number(fcs);
}
var _NODE_ZLIB_MOD = "node:zlib", isBun;
var init_zstd = __esm(() => {
  init_esm();
  isBun = typeof globalThis.Bun !== "undefined";
});

// src/client/tcp.ts
import { connect } from "node:net";

// src/client/pipe.ts
import {
  Field,
  makeData,
  RecordBatch,
  RecordBatchStreamWriter as RecordBatchStreamWriter2,
  Schema,
  Struct,
  vectorFromArray
} from "@query-farm/apache-arrow";

// src/constants.ts
var RPC_METHOD_KEY = "vgi_rpc.method";
var LOG_LEVEL_KEY = "vgi_rpc.log_level";
var LOG_MESSAGE_KEY = "vgi_rpc.log_message";
var LOG_EXTRA_KEY = "vgi_rpc.log_extra";
var REQUEST_VERSION_KEY = "vgi_rpc.request_version";
var REQUEST_VERSION = "1";
var SERVER_ID_KEY = "vgi_rpc.server_id";
var REQUEST_ID_KEY = "vgi_rpc.request_id";
var PROTOCOL_NAME_KEY = "vgi_rpc.protocol_name";
var DESCRIBE_VERSION_KEY = "vgi_rpc.describe_version";
var PROTOCOL_HASH_KEY = "vgi_rpc.protocol_hash";
var DESCRIBE_VERSION = "4";
var PROTOCOL_VERSION_KEY = "vgi_rpc.protocol_version";
var DESCRIBE_METHOD_NAME = "__describe__";
var STATE_KEY = "vgi_rpc.stream_state#b64";
var CALL_STATE_KEY = "vgi_rpc.call_state#b64";
var CANCEL_KEY = "vgi_rpc.cancel";
var LOCATION_KEY = "vgi_rpc.location";
var LOCATION_SHA256_KEY = "vgi_rpc.location.sha256";
var RPC_ERROR_HEADER = "X-VGI-RPC-Error";
var REQUEST_ID_HEADER = "X-Request-ID";
var ERROR_KIND_KEY = "vgi_rpc.error_kind";

// src/errors.ts
class RpcError extends Error {
  errorType;
  errorMessage;
  remoteTraceback;
  constructor(errorType, errorMessage, remoteTraceback) {
    super(`${errorType}: ${errorMessage}`);
    this.errorType = errorType;
    this.errorMessage = errorMessage;
    this.remoteTraceback = remoteTraceback;
    this.name = "RpcError";
  }
}

class VersionError extends Error {
  constructor(message) {
    super(message);
    this.name = "VersionError";
  }
}
var ERROR_KIND_METHOD_NOT_IMPLEMENTED = "method_not_implemented";
var ERROR_KIND_SESSION_LOST = "session_lost";
var ERROR_KIND_SERVER_DRAINING = "server_draining";
var ERROR_KIND_PROTOCOL_VERSION_MISMATCH = "protocol_version_mismatch";

class ProtocolVersionError extends VersionError {
  static errorKind = ERROR_KIND_PROTOCOL_VERSION_MISMATCH;
  errorKind = ERROR_KIND_PROTOCOL_VERSION_MISMATCH;
  constructor(message) {
    super(message);
    this.name = "ProtocolVersionError";
  }
}
var SEMVER_REGEX = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
function parseProtocolVersion(value) {
  const m = SEMVER_REGEX.exec(value);
  if (!m) {
    throw new Error(`Invalid protocol version '${value}': expected canonical semver ` + "MAJOR.MINOR.PATCH with non-negative integers and no leading zeros " + "(no prereleases or build metadata).");
  }
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

class MethodNotImplementedError extends Error {
  static errorKind = ERROR_KIND_METHOD_NOT_IMPLEMENTED;
  errorKind = ERROR_KIND_METHOD_NOT_IMPLEMENTED;
  constructor(message) {
    super(message);
    this.name = "MethodNotImplementedError";
  }
}

class SessionLostError extends Error {
  static errorKind = ERROR_KIND_SESSION_LOST;
  errorKind = ERROR_KIND_SESSION_LOST;
  constructor(message) {
    super(message);
    this.name = "SessionLostError";
  }
}

class ServerDrainingError extends Error {
  static errorKind = ERROR_KIND_SERVER_DRAINING;
  errorKind = ERROR_KIND_SERVER_DRAINING;
  constructor(message) {
    super(message);
    this.name = "ServerDrainingError";
  }
}

// src/arrow/impl-arrowjs/index.ts
import {
  Binary as A_Binary,
  Bool as A_Bool,
  Data as A_Data,
  DataType as A_DataTypeNS,
  DateDay as A_DateDay,
  Decimal as A_Decimal,
  Dictionary as A_Dictionary,
  DurationMicrosecond as A_DurationMicrosecond,
  Field as A_Field,
  FixedSizeBinary as A_FixedSizeBinary,
  Float32 as A_Float32,
  Float64 as A_Float64,
  Int8 as A_Int8,
  Int16 as A_Int16,
  Int32 as A_Int32,
  Int64 as A_Int64,
  LargeBinary as A_LargeBinary,
  LargeUtf8 as A_LargeUtf8,
  List as A_List,
  Map_ as A_Map,
  Null as A_Null,
  RecordBatch as A_RecordBatch,
  Schema as A_Schema,
  Struct as A_Struct,
  TimeMicrosecond as A_TimeMicrosecond,
  Timestamp as A_Timestamp,
  TimeUnit as A_TimeUnit,
  Type as A_Type,
  Uint8 as A_Uint8,
  Uint16 as A_Uint16,
  Uint32 as A_Uint32,
  Uint64 as A_Uint64,
  Utf8 as A_Utf8,
  makeData as a_makeData,
  vectorFromArray as a_vectorFromArray,
  RecordBatchReader,
  RecordBatchStreamWriter
} from "@query-farm/apache-arrow";

// src/arrow/limits.ts
var MAX_ENCODABLE_BYTES = 2147483640;
function isBufferBearing(value) {
  return typeof value === "object" && value !== null && typeof value.buffers === "object";
}
function requireEncodable(value, fieldName) {
  let bytes = -1;
  if (ArrayBuffer.isView(value)) {
    bytes = value.byteLength;
  } else if (value instanceof ArrayBuffer) {
    bytes = value.byteLength;
  } else if (isBufferBearing(value)) {
    for (const buf of Object.values(value.buffers)) {
      const len = buf?.byteLength;
      if (typeof len === "number" && len > bytes)
        bytes = len;
    }
  } else if (typeof value === "string") {
    bytes = value.length;
  }
  if (bytes > MAX_ENCODABLE_BYTES) {
    throw new RangeError(`${fieldName} is ${bytes} bytes; this TypeScript worker can encode at most ${MAX_ENCODABLE_BYTES} ` + `(2 GiB - 8). Both Arrow backends align buffers with (byteLength + 7) & ~7, and the bitwise & ` + `truncates to int32, so a larger value would be sent as a negative bodyLength. The wire and the ` + `protocol carry it fine — the limit is the JavaScript Arrow encoders', not vgi-rpc's.`);
  }
}

// src/arrow/impl-arrowjs/index.ts
var backend = { name: "arrow-js", opaquePassthrough: true };
var bool = () => new A_Bool;
var int8 = () => new A_Int8;
var int16 = () => new A_Int16;
var int32 = () => new A_Int32;
var int64 = () => new A_Int64;
var uint8 = () => new A_Uint8;
var uint16 = () => new A_Uint16;
var uint32 = () => new A_Uint32;
var uint64 = () => new A_Uint64;
var float32 = () => new A_Float32;
var float64 = () => new A_Float64;
var utf8 = () => new A_Utf8;
var binary = () => new A_Binary;
var timestampMicro = (timezone = null) => new A_Timestamp(A_TimeUnit.MICROSECOND, timezone);
function field(name, type, nullable = true, metadata) {
  return new A_Field(name, type, nullable, metadata ?? new Map);
}
function schema(fields, metadata) {
  return new A_Schema(fields, metadata ?? new Map);
}
function serializeSchema(s) {
  const writer = new RecordBatchStreamWriter;
  writer.reset(undefined, s);
  writer.close();
  return writer.toUint8Array(true);
}
function deserializeSchema(bytes) {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length > 0)
    return batches[0].schema;
  if (reader.schema)
    return reader.schema;
  throw new Error("Cannot deserialize schema from empty IPC stream");
}
function serializeBatch(batch) {
  const a = batch;
  const writer = new RecordBatchStreamWriter;
  writer.reset(undefined, a.schema);
  writer._writeRecordBatch(a);
  writer.close();
  return writer.toUint8Array(true);
}
function createIncrementalEncoder(s) {
  const writer = new RecordBatchStreamWriter;
  writer.reset(undefined, s);
  const drain = () => {
    const values = writer._sink._values;
    if (values.length === 1) {
      const only = values[0];
      values.length = 0;
      return only;
    }
    const total = values.reduce((n, c) => n + c.length, 0);
    const out = new Uint8Array(total);
    let off = 0;
    for (const c of values) {
      out.set(c, off);
      off += c.length;
    }
    values.length = 0;
    return out;
  };
  return {
    start: () => drain(),
    writeBatch(batch) {
      writer._writeRecordBatch(batch);
      return drain();
    },
    finish: () => new Uint8Array(new Int32Array([-1, 0]).buffer)
  };
}
function deserializeBatch(bytes) {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length === 0) {
    const sch = reader.schema ?? new A_Schema([]);
    const structType = new A_Struct(sch.fields);
    const data = a_makeData({ type: structType, length: 0, children: [], nullCount: 0 });
    return new A_RecordBatch(sch, data);
  }
  return batches[0];
}
function batchFromColumns(s, columns) {
  const a = s;
  const numRows = a.fields.length > 0 ? columns[a.fields[0].name]?.length ?? 0 : 0;
  const children = a.fields.map((f) => {
    const vals = columns[f.name];
    if (!vals)
      return a_makeData({ type: f.type, length: numRows, nullCount: numRows });
    for (const v of vals)
      requireEncodable(v, f.name);
    return a_vectorFromArray(vals, f.type).data[0];
  });
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: numRows, children, nullCount: 0 });
  return new A_RecordBatch(a, data);
}
function emptyBatchWithMetadata(s, metadata) {
  const a = s;
  const children = a.fields.map((f) => makeEmptyDataRecursive(f.type));
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: 0, children, nullCount: 0 });
  return new A_RecordBatch(a, data, metadata);
}
function makeEmptyDataRecursive(type) {
  const M = { DataType: A_DataTypeNS, Data: A_Data };
  if (M.DataType.isStruct(type)) {
    const children = type.children.map((f) => makeEmptyDataRecursive(f.type));
    return a_makeData({ type, length: 0, children, nullCount: 0 });
  }
  if (M.DataType.isList(type)) {
    const childData = makeEmptyDataRecursive(type.children[0].type);
    return a_makeData({ type, length: 0, child: childData, nullCount: 0, valueOffsets: new Int32Array([0]) });
  }
  if (M.DataType.isFixedSizeList(type)) {
    const childData = makeEmptyDataRecursive(type.children[0].type);
    return a_makeData({ type, length: 0, child: childData, nullCount: 0 });
  }
  if (M.DataType.isMap(type)) {
    const entryType = type.children[0]?.type;
    const entryData = entryType ? makeEmptyDataRecursive(entryType) : a_makeData({ type: new A_Struct([]), length: 0, children: [], nullCount: 0 });
    return a_makeData({ type, length: 0, child: entryData, nullCount: 0, valueOffsets: new Int32Array([0]) });
  }
  if (M.DataType.isUnion(type)) {
    const children = type.children.map((f) => makeEmptyDataRecursive(f.type));
    if (M.DataType.isDenseUnion(type)) {
      return a_makeData({
        type,
        length: 0,
        typeIds: new Int8Array(0),
        valueOffsets: new Int32Array(0),
        children,
        nullCount: 0
      });
    }
    return a_makeData({
      type,
      length: 0,
      typeIds: new Int8Array(0),
      children,
      nullCount: 0
    });
  }
  return a_makeData({ type, length: 0, nullCount: 0 });
}
function singleRowBatchWithMetadata(s, values, metadata) {
  const a = s;
  const M = { DataType: A_DataTypeNS, Data: A_Data };
  const children = a.fields.map((f) => {
    const val = values[f.name];
    requireEncodable(val, f.name);
    if (val instanceof M.Data)
      return val;
    return a_vectorFromArray([val], f.type).data[0];
  });
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: 1, children, nullCount: 0 });
  return new A_RecordBatch(a, data, metadata);
}
function withBatchMetadata(batch, metadata) {
  const a = batch;
  return new A_RecordBatch(a.schema, a.data, metadata);
}
function serializeBatches(schema2, batches) {
  const writer = new RecordBatchStreamWriter;
  writer.reset(undefined, schema2);
  for (const batch of batches) {
    writer._writeRecordBatch(batch);
  }
  writer.close();
  return writer.toUint8Array(true);
}
var _needsValueCast = (src, dst) => {
  if (src.typeId === dst.typeId)
    return false;
  if (src.constructor === dst.constructor)
    return false;
  return true;
};
var _isNumeric = (t) => t.typeId === A_Type.Int || t.typeId === A_Type.Float;
function conformBatchToSchema(batch, schema2) {
  const a = batch;
  if (a.numRows === 0)
    return batch;
  const s = schema2;
  if (a.schema.fields.length !== s.fields.length) {
    throw new TypeError(`Field count mismatch: expected ${s.fields.length}, got ${a.schema.fields.length}`);
  }
  for (let i = 0;i < s.fields.length; i++) {
    if (a.schema.fields[i].name !== s.fields[i].name) {
      throw new TypeError(`Field name mismatch at index ${i}: expected '${s.fields[i].name}', got '${a.schema.fields[i].name}'`);
    }
  }
  let mutated = false;
  const children = s.fields.map((f, i) => {
    const srcChild = a.data.children[i];
    const srcType = srcChild.type;
    const dstType = f.type;
    if (srcType === dstType)
      return srcChild;
    if (!_needsValueCast(srcType, dstType)) {
      mutated = true;
      return srcChild.clone(dstType);
    }
    if (_isNumeric(srcType) && _isNumeric(dstType)) {
      mutated = true;
      const col = a.getChildAt(i);
      const values = [];
      for (let r = 0;r < a.numRows; r++) {
        const v = col.get(r);
        values.push(typeof v === "bigint" ? Number(v) : v);
      }
      return a_vectorFromArray(values, dstType).data[0];
    }
    mutated = true;
    return srcChild.clone(dstType);
  });
  if (!mutated)
    return batch;
  const structType = new A_Struct(s.fields);
  const data = a_makeData({
    type: structType,
    length: a.numRows,
    children,
    nullCount: a.data.nullCount,
    nullBitmap: a.data.nullBitmap
  });
  return new A_RecordBatch(s, data, a.metadata);
}
// src/arrow/predicates.ts
var TypeId = {
  Null: 1,
  Int: 2,
  Float: 3,
  Binary: 4,
  Utf8: 5,
  Bool: 6,
  Decimal: 7,
  Date: 8,
  Time: 9,
  Timestamp: 10,
  Interval: 11,
  List: 12,
  Struct: 13,
  Union: 14,
  FixedSizeBinary: 15,
  FixedSizeList: 16,
  Map: 17,
  Duration: 18,
  LargeBinary: 19,
  LargeUtf8: 20,
  Dictionary: -1
};
var isInt = (t) => t.typeId === TypeId.Int;
var isFloat = (t) => t.typeId === TypeId.Float;
var isBinary = (t) => t.typeId === TypeId.Binary || t.typeId === TypeId.LargeBinary;
var isUtf8 = (t) => t.typeId === TypeId.Utf8 || t.typeId === TypeId.LargeUtf8;
var isLargeUtf8 = (t) => t.typeId === TypeId.LargeUtf8;
var isLargeBinary = (t) => t.typeId === TypeId.LargeBinary;
var isBool = (t) => t.typeId === TypeId.Bool;
var isDecimal = (t) => t.typeId === TypeId.Decimal;
var isDate = (t) => t.typeId === TypeId.Date;
var isTime = (t) => t.typeId === TypeId.Time;
var isTimestamp = (t) => t.typeId === TypeId.Timestamp;
var isDuration = (t) => t.typeId === TypeId.Duration;
var isMap = (t) => t.typeId === TypeId.Map;
var isFixedSizeBinary = (t) => t.typeId === TypeId.FixedSizeBinary;
var isDictionary = (t) => t.typeId === TypeId.Dictionary;
function isBatch(x) {
  return x != null && typeof x.numRows === "number" && x.schema != null && Array.isArray(x.schema.fields);
}
// src/external.ts
init_zstd();

// src/wire/response.ts
var _int64FieldsCache = new WeakMap;
function int64FieldNames(schema2) {
  let names = _int64FieldsCache.get(schema2);
  if (names === undefined) {
    const out = [];
    for (const f of schema2.fields) {
      if (isInt(f.type) && f.type.bitWidth === 64)
        out.push(f.name);
    }
    names = out;
    _int64FieldsCache.set(schema2, names);
  }
  return names;
}
function coerceInt64(schema2, values) {
  const int64Fields = int64FieldNames(schema2);
  if (int64Fields.length === 0)
    return values;
  let result = null;
  for (const name of int64Fields) {
    const val = values[name];
    if (val === undefined)
      continue;
    if (Array.isArray(val)) {
      let mapped = null;
      for (let i = 0;i < val.length; i++) {
        if (typeof val[i] === "number") {
          if (mapped === null)
            mapped = val.slice();
          mapped[i] = BigInt(val[i]);
        }
      }
      if (mapped !== null) {
        result ??= { ...values };
        result[name] = mapped;
      }
    } else if (typeof val === "number") {
      result ??= { ...values };
      result[name] = BigInt(val);
    }
  }
  return result ?? values;
}
function buildResultBatch(schema2, values, serverId, requestId) {
  const metadata = new Map;
  metadata.set(SERVER_ID_KEY, serverId);
  if (requestId !== null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }
  if (schema2.fields.length === 0) {
    return buildEmptyBatch(schema2, metadata);
  }
  for (const f of schema2.fields) {
    if (values[f.name] === undefined && !f.nullable) {
      const got = Object.keys(values);
      throw new TypeError(`Handler result missing required field '${f.name}'. Got keys: [${got.join(", ")}]`);
    }
  }
  const coerced = coerceInt64(schema2, values);
  return singleRowBatchWithMetadata(schema2, coerced, metadata);
}
function buildErrorBatch(schema2, error, serverId, requestId) {
  const metadata = new Map;
  metadata.set(LOG_LEVEL_KEY, "EXCEPTION");
  const exceptionType = typeof error.name === "string" && error.name !== "Error" ? error.name : error.constructor.name;
  metadata.set(LOG_MESSAGE_KEY, `${exceptionType}: ${error.message}`);
  const errorKind = error.errorKind ?? error.constructor.errorKind;
  if (typeof errorKind === "string" && errorKind.length > 0) {
    metadata.set(ERROR_KIND_KEY, errorKind);
  }
  const extra = {
    exception_type: exceptionType,
    exception_message: error.message,
    traceback: error.stack ?? ""
  };
  if (typeof errorKind === "string" && errorKind.length > 0) {
    extra.error_kind = errorKind;
  }
  metadata.set(LOG_EXTRA_KEY, JSON.stringify(extra));
  metadata.set(SERVER_ID_KEY, serverId);
  if (requestId !== null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }
  return buildEmptyBatch(schema2, metadata);
}
function buildLogBatch(schema2, level, message, extra, serverId, requestId) {
  const metadata = new Map;
  metadata.set(LOG_LEVEL_KEY, level);
  metadata.set(LOG_MESSAGE_KEY, message);
  if (extra) {
    metadata.set(LOG_EXTRA_KEY, JSON.stringify(extra));
  }
  if (serverId != null) {
    metadata.set(SERVER_ID_KEY, serverId);
  }
  if (requestId != null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }
  return buildEmptyBatch(schema2, metadata);
}
function buildEmptyBatch(schema2, metadata) {
  return emptyBatchWithMetadata(schema2, metadata);
}

// src/external.ts
var DEFAULT_THRESHOLD = 1048576;
function httpsOnlyValidator(url) {
  const parsed = new URL(url);
  if (parsed.protocol !== "https:") {
    throw new Error(`External location URL must use HTTPS, got "${parsed.protocol}"`);
  }
}
async function sha256Hex(data) {
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  const hash = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hash)).map((b) => b.toString(16).padStart(2, "0")).join("");
}
function isExternalLocationBatch(batch) {
  if (batch.numRows !== 0)
    return false;
  const meta = batch.metadata;
  if (!meta)
    return false;
  return meta.has(LOCATION_KEY) && !meta.has(LOG_LEVEL_KEY);
}
function makeExternalLocationBatch(schema2, url, sha256) {
  const metadata = new Map;
  metadata.set(LOCATION_KEY, url);
  if (sha256) {
    metadata.set(LOCATION_SHA256_KEY, sha256);
  }
  return buildEmptyBatch(schema2, metadata);
}
function serializeBatchToIpc(batch) {
  return serializeBatch(batch);
}
function batchByteSize(batch) {
  return serializeBatch(batch).byteLength;
}
async function maybeExternalizeBatch(batch, config, onUpload) {
  if (!config?.storage)
    return batch;
  if (batch.numRows === 0)
    return batch;
  const threshold = config.externalizeThresholdBytes ?? DEFAULT_THRESHOLD;
  if (batchByteSize(batch) < threshold)
    return batch;
  let ipcData = serializeBatchToIpc(batch);
  const checksum = await sha256Hex(ipcData);
  let contentEncoding = "";
  if (config.compression?.algorithm === "zstd") {
    ipcData = await zstdCompress(ipcData, config.compression.level ?? 3);
    contentEncoding = "zstd";
  }
  onUpload?.(ipcData.byteLength);
  const url = await config.storage.upload(ipcData, contentEncoding);
  return makeExternalLocationBatch(batch.schema, url, checksum);
}
async function resolveExternalLocation(batch, config) {
  if (!config)
    return batch;
  if (!isExternalLocationBatch(batch))
    return batch;
  const url = batch.metadata?.get(LOCATION_KEY);
  if (!url)
    return batch;
  const validator = config.urlValidator === null ? undefined : config.urlValidator ?? httpsOnlyValidator;
  if (validator) {
    validator(url);
  }
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`External location fetch failed: ${response.status} ${response.statusText} [url: ${url}]`);
  }
  let data = new Uint8Array(await response.arrayBuffer());
  const contentEncoding = response.headers.get("Content-Encoding");
  if (contentEncoding === "zstd") {
    const cap = data.byteLength * 16;
    data = new Uint8Array(await zstdDecompress(data, cap));
  }
  const expectedSha256 = batch.metadata?.get(LOCATION_SHA256_KEY);
  if (expectedSha256) {
    const actualSha256 = await sha256Hex(data);
    if (actualSha256 !== expectedSha256) {
      throw new Error(`SHA-256 checksum mismatch for ${url}: expected ${expectedSha256}, got ${actualSha256}`);
    }
  }
  const resolved = deserializeBatch(data);
  if (resolved.numRows === 0 && resolved.schema.fields.length === 0) {
    throw new Error(`No data batch found in external IPC stream from ${url}`);
  }
  return resolved;
}

// src/util/gzip.ts
async function streamThrough(data, transform, maxOutputSize) {
  const ws = transform.writable.getWriter();
  const rs = transform.readable.getReader();
  const view = new Uint8Array(data.byteLength);
  view.set(data);
  const writePromise = (async () => {
    await ws.write(view);
    await ws.close();
  })();
  const chunks = [];
  let total = 0;
  while (true) {
    const { value, done } = await rs.read();
    if (done)
      break;
    const chunk = value;
    total += chunk.byteLength;
    if (maxOutputSize != null && total > maxOutputSize) {
      throw new Error(`gzip decompressed size (${total}) exceeds cap (${maxOutputSize})`);
    }
    chunks.push(chunk);
  }
  await writePromise;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}
async function gzipDecompress(data, maxOutputSize) {
  return streamThrough(data, new DecompressionStream("gzip"), maxOutputSize);
}
async function gzipCompress(data, _level) {
  return streamThrough(data, new CompressionStream("gzip"));
}

// src/http/common.ts
init_zstd();
var ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";
var UPLOAD_URL_METHOD = "__upload_url__";
var MAX_UPLOAD_URL_COUNT = 100;
var UPLOAD_URL_PARAMS_SCHEMA = schema([field("count", int64(), true)]);
var UPLOAD_URL_RESPONSE_SCHEMA = schema([
  field("upload_url", utf8(), false),
  field("download_url", utf8(), false),
  field("expires_at", timestampMicro("UTC"), false)
]);
async function decodeContentEncoding(data, contentEncoding, maxOutputSize) {
  if (!contentEncoding)
    return data;
  const codings = contentEncoding.split(",").map((c) => c.trim().toLowerCase()).filter((c) => c.length > 0).reverse();
  let result = data;
  for (const coding of codings) {
    if (coding === "zstd")
      result = await zstdDecompress(result, maxOutputSize);
    else if (coding === "gzip")
      result = await gzipDecompress(result, maxOutputSize);
  }
  return result;
}
var SESSION_HEADER = "VGI-Session";
var SESSION_ACCEPT_HEADER = "VGI-Session-Accept";
var SESSION_CLOSE_HEADER = "VGI-Session-Close";
var STICKY_ENABLED_HEADER = "VGI-Sticky-Enabled";
var STICKY_DEFAULT_TTL_HEADER = "VGI-Sticky-Default-TTL";
var STICKY_ECHO_HEADERS_HEADER = "VGI-Sticky-Echo-Headers";
var ECHO_HEADER_PREFIX = "VGI-Echo-";
var SESSION_ENDPOINT = "__session__";
function formatSetCookieHeader(c) {
  const parts = [];
  if (c.delete) {
    parts.push(`${c.name}=`);
    parts.push("Max-Age=0");
  } else {
    parts.push(`${c.name}=${c.value}`);
    if (c.maxAge !== undefined)
      parts.push(`Max-Age=${c.maxAge}`);
    if (c.expires)
      parts.push(`Expires=${c.expires.toUTCString()}`);
  }
  if (c.path)
    parts.push(`Path=${c.path}`);
  if (c.domain)
    parts.push(`Domain=${c.domain}`);
  if (c.secure)
    parts.push("Secure");
  if (c.httpOnly)
    parts.push("HttpOnly");
  if (c.sameSite)
    parts.push(`SameSite=${c.sameSite}`);
  if (c.partitioned)
    parts.push("Partitioned");
  return parts.join("; ");
}
function appendCookieHeaders(headers, cookies) {
  for (const c of cookies) {
    headers.append("Set-Cookie", formatSetCookieHeader(c));
  }
}

class HttpRpcError extends Error {
  statusCode;
  constructor(message, statusCode) {
    super(message);
    this.statusCode = statusCode;
    this.name = "HttpRpcError";
  }
}
function serializeIpcStream(schema2, batches) {
  const conformed = batches.map((b) => conformBatchToSchema(b, schema2));
  return serializeBatches(schema2, conformed);
}
function arrowResponse(body, status = 200, extraHeaders) {
  const headers = extraHeaders ?? new Headers;
  headers.set("Content-Type", ARROW_CONTENT_TYPE);
  if (status === 500) {
    headers.set(RPC_ERROR_HEADER, "true");
    return new Response(body, { status: 200, headers });
  }
  return new Response(body, { status, headers });
}
async function readRequestFromBody(body) {
  const batch = deserializeBatch(body);
  if (batch.schema.fields.length === 0 && batch.numRows === 0 && (batch.metadata?.size ?? 0) === 0) {
    throw new HttpRpcError("Empty IPC stream: no schema", 400);
  }
  return { schema: batch.schema, batch };
}

// src/wire/reader.ts
import { RecordBatchReader as RecordBatchReader2 } from "@query-farm/apache-arrow";
var MAX_READ_CHUNK = 1 << 26;
function isNodeReadable(input) {
  const s = input;
  return typeof s?.read === "function" && typeof s?.pipe === "function";
}
function clampReads(stream) {
  return new Proxy(stream, {
    get(target, prop) {
      if (prop === "read") {
        return (size) => target.read(typeof size === "number" ? Math.min(size, MAX_READ_CHUNK) : size);
      }
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    }
  });
}

class IpcStreamReader {
  reader;
  initialized = false;
  streamEnded = false;
  constructor(reader) {
    this.reader = reader;
  }
  static async create(input) {
    const source = isNodeReadable(input) ? clampReads(input) : input;
    const reader = await RecordBatchReader2.from(source);
    await reader.open({ autoDestroy: false });
    if (reader.closed) {
      throw new Error("Input stream closed before first IPC message");
    }
    return new IpcStreamReader(reader);
  }
  async readStream() {
    if (this.initialized) {
      await this.reader.reset().open();
      if (this.reader.closed) {
        return null;
      }
    }
    this.initialized = true;
    const schema2 = this.reader.schema;
    if (!schema2) {
      return null;
    }
    const batches = [];
    while (true) {
      const result = await this.reader.next();
      if (result.done)
        break;
      if (result.value.constructor.name === "_InternalEmptyPlaceholderRecordBatch")
        break;
      batches.push(result.value);
    }
    return { schema: schema2, batches };
  }
  async openNextStream() {
    if (this.initialized) {
      await this.reader.reset().open();
      if (this.reader.closed) {
        return null;
      }
    }
    this.initialized = true;
    this.streamEnded = false;
    return this.reader.schema ?? null;
  }
  async readNextBatch() {
    if (this.streamEnded)
      return null;
    const result = await this.reader.next();
    if (result.done) {
      this.streamEnded = true;
      return null;
    }
    if (result.value.constructor.name === "_InternalEmptyPlaceholderRecordBatch") {
      this.streamEnded = true;
      return null;
    }
    return result.value;
  }
  async cancel() {
    await this.reader.cancel();
  }
}

// src/wire/writer.ts
var STDOUT_FD = 1;
var RESOLVED = Promise.resolve();
var _NODE_FS_MOD = "node:fs";
var _writeSync = null;
function _loadWriteSync() {
  if (_writeSync)
    return _writeSync;
  const getBuiltin = globalThis.process?.getBuiltinModule;
  if (typeof getBuiltin === "function") {
    const fs2 = getBuiltin.call(globalThis.process, _NODE_FS_MOD);
    if (fs2?.writeSync) {
      _writeSync = fs2.writeSync.bind(fs2);
      return _writeSync;
    }
  }
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req) {
    throw new Error("IpcStreamWriter needs synchronous node:fs.writeSync, reached via " + "import.meta.require (Bun), globalThis.require (Node CJS), or " + "process.getBuiltinModule (Node >= 20.16). This runtime offers none of " + "them, so the subprocess transport is unavailable. On an older Node ESM, " + "either upgrade or set globalThis.require = createRequire(import.meta.url).");
  }
  const fs = req(_NODE_FS_MOD);
  _writeSync = fs.writeSync.bind(fs);
  return _writeSync;
}
var MAX_WRITE_CHUNK = 1 << 30;
var MAX_STREAM_CHUNK = 128 * 1024;
function writeAll(fd, data) {
  const writeSync = _loadWriteSync();
  let offset = 0;
  let spins = 0;
  while (offset < data.length) {
    try {
      const written = writeSync(fd, data, offset, Math.min(data.length - offset, MAX_WRITE_CHUNK));
      if (written <= 0)
        throw new Error(`writeSync returned ${written}`);
      offset += written;
      spins = 0;
    } catch (e) {
      if (e.code === "EAGAIN") {
        if (++spins < 8192)
          continue;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
        spins = 0;
        continue;
      }
      throw e;
    }
  }
}
async function socketWriteAll(socket, data) {
  let offset = 0;
  do {
    const end = Math.min(offset + MAX_STREAM_CHUNK, data.length);
    await socketWriteChunk(socket, data.subarray(offset, end));
    offset = end;
  } while (offset < data.length);
}
async function socketWriteChunk(socket, data) {
  if (socket.destroyed || socket.writableEnded) {
    throw new Error("socketWriteAll: socket is already closed");
  }
  const ok = socket.write(data);
  if (ok)
    return;
  await new Promise((resolve, reject) => {
    const cleanup = () => {
      socket.off("drain", onDrain);
      socket.off("error", onError);
      socket.off("close", onClose);
    };
    const onDrain = () => {
      cleanup();
      resolve();
    };
    const onError = (err2) => {
      cleanup();
      reject(err2);
    };
    const onClose = () => {
      cleanup();
      resolve();
    };
    socket.once("drain", onDrain);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

class IpcStreamWriter {
  target;
  constructor(fdOrSocketOrSink = STDOUT_FD) {
    if (typeof fdOrSocketOrSink === "number") {
      this.target = { kind: "fd", fd: fdOrSocketOrSink };
    } else if (typeof fdOrSocketOrSink.write === "function" && !("writable" in fdOrSocketOrSink)) {
      this.target = { kind: "sink", sink: fdOrSocketOrSink };
    } else {
      this.target = { kind: "socket", socket: fdOrSocketOrSink };
    }
  }
  async writeStream(schema2, batches) {
    const bytes = serializeBatches(schema2, batches);
    if (this.target.kind === "fd") {
      writeAll(this.target.fd, bytes);
    } else if (this.target.kind === "sink") {
      await this.target.sink.write(bytes);
    } else {
      await socketWriteAll(this.target.socket, bytes);
    }
  }
  openStream(schema2) {
    return new IncrementalStream(this.target, schema2);
  }
}

class IncrementalStream {
  encoder;
  target;
  closed = false;
  writeChain = Promise.resolve();
  constructor(target, schema2) {
    this.target = target;
    this.encoder = createIncrementalEncoder(schema2);
    this.enqueue(this.encoder.start());
  }
  async write(batch) {
    if (this.closed)
      throw new Error("Stream already closed");
    return this.enqueue(this.encoder.writeBatch(batch));
  }
  async close() {
    if (this.closed)
      return;
    this.closed = true;
    return this.enqueue(this.encoder.finish());
  }
  enqueue(bytes) {
    const target = this.target;
    if (target.kind === "fd") {
      writeAll(target.fd, bytes);
      return RESOLVED;
    }
    const next = this.writeChain.then(() => {
      if (target.kind === "sink") {
        return target.sink.write(bytes);
      }
      return socketWriteAll(target.socket, bytes);
    });
    this.writeChain = next.catch(() => {
      return;
    });
    return next;
  }
}

// src/client/introspect.ts
import { Schema as ArrowSchema } from "@query-farm/apache-arrow";

// src/http/codec.ts
var DEFAULT_COMPRESSION_LEVEL = 1;
var COMPRESSION_ENCODINGS = ["zstd", "gzip"];
var IDENTITY_ENCODING = "identity";
var KNOWN_ENCODINGS = [...COMPRESSION_ENCODINGS, IDENTITY_ENCODING];
function parseEncodingList(headerValue) {
  if (!headerValue)
    return [];
  const out = [];
  const seen = new Set;
  for (const raw of headerValue.split(",")) {
    let token = raw.trim().toLowerCase();
    if (!token)
      continue;
    const semi = token.indexOf(";");
    if (semi >= 0)
      token = token.slice(0, semi).trim();
    if (!KNOWN_ENCODINGS.includes(token))
      continue;
    if (seen.has(token))
      continue;
    seen.add(token);
    out.push(token);
  }
  return out;
}
function pickResponseEncoding(standardHeader, customHeader, canProduce) {
  const standard = parseEncodingList(standardHeader);
  const custom = parseEncodingList(customHeader);
  const merged = [...custom, ...standard.filter((e) => !custom.includes(e))];
  for (const enc of merged) {
    if (enc === IDENTITY_ENCODING) {
      return { codec: null, usedCustom: false };
    }
    if (canProduce(enc)) {
      return { codec: enc, usedCustom: custom.includes(enc) && !standard.includes(enc) };
    }
  }
  return { codec: null, usedCustom: custom.length > 0 };
}
var CONTENT_ENCODING_HEADER = "Content-Encoding";
var VGI_CONTENT_ENCODING_HEADER = "X-VGI-Content-Encoding";
var VGI_ACCEPT_ENCODING_HEADER = "X-VGI-Accept-Encoding";
var SUPPORTED_ENCODINGS_HEADER = "VGI-Supported-Encodings";

// src/client/decode.ts
function resolveResponseEncoding(headers) {
  const custom = headers.get(VGI_CONTENT_ENCODING_HEADER)?.trim().toLowerCase();
  if (custom && custom !== "identity") {
    return { codec: custom, custom: true };
  }
  const standard = headers.get(CONTENT_ENCODING_HEADER)?.trim().toLowerCase();
  if (standard === "zstd") {
    return { codec: "zstd", custom: false };
  }
  return { codec: null, custom: false };
}
async function decodeResponseBody(headers, body, zstdDecompress2) {
  const { codec, custom } = resolveResponseEncoding(headers);
  if (!codec)
    return body;
  if (codec === "gzip") {
    return new Uint8Array(await gzipDecompress(body));
  }
  if (codec === "zstd") {
    if (!zstdDecompress2) {
      throw new RpcError("ProtocolError", "Server sent a zstd-encoded response but this client has no zstd decoder. " + "Install the optional zstd dependency, or configure the server not to negotiate zstd.", "");
    }
    return new Uint8Array(await zstdDecompress2(body));
  }
  throw new RpcError("ProtocolError", `Unsupported response encoding '${codec}'` + `${custom ? ` (${VGI_CONTENT_ENCODING_HEADER})` : ` (${CONTENT_ENCODING_HEADER})`}.`, "");
}

// src/client/ipc.ts
import {
  Binary,
  Bool,
  DataType,
  Float64,
  Int64,
  RecordBatchReader as RecordBatchReader3,
  Utf8
} from "@query-farm/apache-arrow";
function inferArrowType(value) {
  if (typeof value === "string")
    return new Utf8;
  if (typeof value === "boolean")
    return new Bool;
  if (typeof value === "bigint")
    return new Int64;
  if (typeof value === "number")
    return new Float64;
  if (value instanceof Uint8Array)
    return new Binary;
  return new Utf8;
}
function coerceForArrow(type, value) {
  if (value == null)
    return value;
  if (DataType.isInt(type) && type.bitWidth === 64) {
    if (typeof value === "number")
      return BigInt(value);
    return value;
  }
  if (DataType.isMap(type)) {
    if (value instanceof Map) {
      const entriesField = type.children[0];
      const valueType = entriesField.type.children[1].type;
      const coerced = new Map;
      for (const [k, v] of value) {
        coerced.set(k, coerceForArrow(valueType, v));
      }
      return coerced;
    }
    return value;
  }
  if (DataType.isList(type)) {
    if (Array.isArray(value)) {
      const elemType = type.children[0].type;
      return value.map((v) => coerceForArrow(elemType, v));
    }
    return value;
  }
  return value;
}
function buildRequestIpc(schema2, params, method, options) {
  const metadata = new Map;
  metadata.set(RPC_METHOD_KEY, method);
  metadata.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  if (options?.protocolVersion) {
    metadata.set(PROTOCOL_VERSION_KEY, options.protocolVersion);
  }
  if (schema2.fields.length === 0) {
    const batch2 = emptyBatchWithMetadata(schema2, metadata);
    return serializeIpcStream(schema2, [batch2]);
  }
  const coerced = {};
  for (const f of schema2.fields) {
    const raw = params[f.name];
    coerced[f.name] = raw === undefined ? null : coerceForArrow(f.type, raw);
  }
  const batch = singleRowBatchWithMetadata(schema2, coerced, metadata);
  return serializeIpcStream(schema2, [batch]);
}
async function readResponseBatches(body) {
  const reader = await RecordBatchReader3.from(body);
  await reader.open();
  const schema2 = reader.schema;
  if (!schema2) {
    throw new RpcError("ProtocolError", "Empty IPC stream: no schema", "");
  }
  const batches = reader.readAll();
  return { schema: schema2, batches };
}
function dispatchLogOrError(batch, onLog) {
  const meta = batch.metadata;
  if (!meta)
    return false;
  const level = meta.get(LOG_LEVEL_KEY);
  if (!level)
    return false;
  const message = meta.get(LOG_MESSAGE_KEY) ?? "";
  if (level === "EXCEPTION") {
    const extraStr = meta.get(LOG_EXTRA_KEY);
    let errorType = "RpcError";
    let errorMessage = message;
    let traceback = "";
    if (extraStr) {
      try {
        const extra = JSON.parse(extraStr);
        errorType = extra.exception_type ?? "RpcError";
        errorMessage = extra.exception_message ?? message;
        traceback = extra.traceback ?? "";
      } catch {}
    }
    throw new RpcError(errorType, errorMessage, traceback);
  }
  if (onLog) {
    const extraStr = meta.get(LOG_EXTRA_KEY);
    let extra;
    if (extraStr) {
      try {
        extra = JSON.parse(extraStr);
      } catch {}
    }
    onLog({ level, message, extra });
  }
  return true;
}
function extractBatchRows(batch) {
  const rows = [];
  for (let r = 0;r < batch.numRows; r++) {
    const row = {};
    for (let i = 0;i < batch.schema.fields.length; i++) {
      const field2 = batch.schema.fields[i];
      let value = batch.getChildAt(i)?.get(r);
      if (typeof value === "bigint") {
        if (value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
          value = Number(value);
        }
      }
      row[field2.name] = value;
    }
    rows.push(row);
  }
  return rows;
}
async function readSequentialStreams(body) {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(body);
      controller.close();
    }
  });
  return IpcStreamReader.create(stream);
}

// src/client/introspect.ts
function deserializeSchema2(bytes) {
  return deserializeSchema(bytes);
}
async function parseDescribeResponse(batches, onLog) {
  let dataBatch = null;
  for (const batch of batches) {
    if (batch.numRows === 0) {
      dispatchLogOrError(batch, onLog);
      continue;
    }
    dataBatch = batch;
  }
  if (!dataBatch) {
    throw new Error("Empty __describe__ response");
  }
  const meta = dataBatch.metadata;
  const protocolName = meta?.get(PROTOCOL_NAME_KEY) ?? "";
  const protocolVersion = meta?.get(PROTOCOL_VERSION_KEY) ?? "";
  const methods = [];
  for (let i = 0;i < dataBatch.numRows; i++) {
    const name = dataBatch.getChildAt(0).get(i);
    const methodType = dataBatch.getChildAt(1).get(i);
    const _hasReturn = dataBatch.getChildAt(2).get(i);
    const paramsIpc = dataBatch.getChildAt(3).get(i);
    const resultIpc = dataBatch.getChildAt(4).get(i);
    const hasHeader = dataBatch.getChildAt(5).get(i);
    const headerIpc = dataBatch.getChildAt(6)?.get(i);
    const paramsSchema = await deserializeSchema2(paramsIpc);
    const resultSchema = await deserializeSchema2(resultIpc);
    const info = {
      name,
      type: methodType,
      paramsSchema,
      resultSchema
    };
    if (methodType === "stream") {
      info.outputSchema = resultSchema;
    }
    if (hasHeader && headerIpc) {
      info.headerSchema = await deserializeSchema2(headerIpc);
    }
    methods.push(info);
  }
  return { protocolName, protocolVersion, methods };
}
async function httpIntrospect(baseUrl, options) {
  const prefix = options?.prefix ?? "";
  const emptySchema = new ArrowSchema([]);
  const body = buildRequestIpc(emptySchema, {}, DESCRIBE_METHOD_NAME);
  const headers = { "Content-Type": ARROW_CONTENT_TYPE };
  if (options?.authorization) {
    headers.Authorization = options.authorization;
  }
  const level = options?.compressionLevel;
  const compressFn = options?.compressFn;
  const decompressFn = options?.decompressFn;
  let sendBody = body;
  if (level != null && compressFn) {
    headers["Content-Encoding"] = "zstd";
    sendBody = await compressFn(body, level);
  }
  if (level != null && decompressFn) {
    headers["Accept-Encoding"] = "zstd";
  }
  const response = await fetch(`${baseUrl}${prefix}/${DESCRIBE_METHOD_NAME}`, {
    method: "POST",
    headers,
    body: sendBody
  });
  if (response.status === 401) {
    throw new RpcError("AuthenticationError", "Authentication required", "");
  }
  const rawBody = new Uint8Array(await response.arrayBuffer());
  const responseBody = new Uint8Array(await decodeResponseBody(response.headers, rawBody, decompressFn));
  const { batches } = await readResponseBatches(responseBody);
  return parseDescribeResponse(batches);
}

// src/client/pipe.ts
class PipeIncrementalWriter {
  writer;
  writeFn;
  closed = false;
  constructor(writeFn, schema2) {
    this.writeFn = writeFn;
    this.writer = new RecordBatchStreamWriter2;
    this.writer.reset(undefined, schema2);
    this.drain();
  }
  write(batch) {
    if (this.closed)
      throw new Error("PipeIncrementalWriter already closed");
    this.writer._writeRecordBatch(batch);
    this.drain();
  }
  close() {
    if (this.closed)
      return;
    this.closed = true;
    const eos = new Uint8Array(new Int32Array([-1, 0]).buffer);
    this.writeFn(eos);
  }
  drain() {
    const values = this.writer._sink._values;
    for (const chunk of values) {
      this.writeFn(chunk);
    }
    values.length = 0;
  }
}

class PipeStreamSession {
  _reader;
  _writeFn;
  _onLog;
  _header;
  _inputWriter = null;
  _inputSchema = null;
  _outputStreamOpened = false;
  _closed = false;
  _outputSchema;
  _releaseBusy;
  _setDrainPromise;
  _externalConfig;
  constructor(opts) {
    this._reader = opts.reader;
    this._writeFn = opts.writeFn;
    this._onLog = opts.onLog;
    this._header = opts.header;
    this._outputSchema = opts.outputSchema;
    this._releaseBusy = opts.releaseBusy;
    this._setDrainPromise = opts.setDrainPromise;
    this._externalConfig = opts.externalConfig;
  }
  get header() {
    return this._header;
  }
  async _readOutputBatch() {
    while (true) {
      const batch = await this._reader.readNextBatch();
      if (batch === null)
        return null;
      if (batch.numRows === 0) {
        if (isExternalLocationBatch(batch)) {
          return await resolveExternalLocation(batch, this._externalConfig);
        }
        if (dispatchLogOrError(batch, this._onLog)) {
          continue;
        }
      }
      return batch;
    }
  }
  async _ensureOutputStream() {
    if (this._outputStreamOpened)
      return;
    this._outputStreamOpened = true;
    const schema2 = await this._reader.openNextStream();
    if (!schema2) {
      throw new RpcError("ProtocolError", "Expected output stream but got EOF", "");
    }
  }
  async exchange(input) {
    if (this._closed) {
      throw new RpcError("ProtocolError", "Stream session is closed", "");
    }
    let inputSchema;
    let batch;
    if (input.length === 0) {
      inputSchema = this._inputSchema ?? this._outputSchema;
      const children = inputSchema.fields.map((f) => {
        return makeData({ type: f.type, length: 0, nullCount: 0 });
      });
      const structType = new Struct(inputSchema.fields);
      const data = makeData({
        type: structType,
        length: 0,
        children,
        nullCount: 0
      });
      batch = new RecordBatch(inputSchema, data);
    } else {
      const keys = Object.keys(input[0]);
      const fields = keys.map((key) => {
        let sample;
        for (const row of input) {
          if (row[key] != null) {
            sample = row[key];
            break;
          }
        }
        const arrowType = inferArrowType(sample);
        return new Field(key, arrowType, true);
      });
      inputSchema = new Schema(fields);
      if (this._inputSchema) {
        const cached = this._inputSchema;
        if (cached.fields.length !== inputSchema.fields.length || cached.fields.some((f, i) => f.name !== inputSchema.fields[i].name)) {
          throw new RpcError("ProtocolError", `Exchange input schema changed: expected [${cached.fields.map((f) => f.name).join(", ")}] ` + `but got [${inputSchema.fields.map((f) => f.name).join(", ")}]`, "");
        }
      } else {
        this._inputSchema = inputSchema;
      }
      const children = inputSchema.fields.map((f) => {
        const values = input.map((row) => row[f.name]);
        return vectorFromArray(values, f.type).data[0];
      });
      const structType = new Struct(inputSchema.fields);
      const data = makeData({
        type: structType,
        length: input.length,
        children,
        nullCount: 0
      });
      batch = new RecordBatch(inputSchema, data);
    }
    if (!this._inputWriter) {
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, inputSchema);
    }
    this._inputWriter.write(batch);
    await this._ensureOutputStream();
    try {
      const outputBatch = await this._readOutputBatch();
      if (outputBatch === null) {
        return [];
      }
      return extractBatchRows(outputBatch);
    } catch (e) {
      await this._cleanup();
      throw e;
    }
  }
  async _cleanup() {
    if (this._closed)
      return;
    this._closed = true;
    if (this._inputWriter) {
      this._inputWriter.close();
      this._inputWriter = null;
    }
    try {
      if (this._outputStreamOpened) {
        while (await this._reader.readNextBatch() !== null) {}
      }
    } catch {}
    this._releaseBusy();
  }
  async* [Symbol.asyncIterator]() {
    if (this._closed)
      return;
    try {
      const tickSchema = new Schema([]);
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, tickSchema);
      const structType = new Struct(tickSchema.fields);
      const tickData = makeData({
        type: structType,
        length: 0,
        children: [],
        nullCount: 0
      });
      const tickBatch = new RecordBatch(tickSchema, tickData);
      while (true) {
        this._inputWriter.write(tickBatch);
        await this._ensureOutputStream();
        const outputBatch = await this._readOutputBatch();
        if (outputBatch === null) {
          break;
        }
        yield extractBatchRows(outputBatch);
      }
    } finally {
      if (this._inputWriter) {
        this._inputWriter.close();
        this._inputWriter = null;
      }
      try {
        if (this._outputStreamOpened) {
          while (await this._reader.readNextBatch() !== null) {}
        }
      } catch {}
      this._closed = true;
      this._releaseBusy();
    }
  }
  close() {
    if (this._closed)
      return;
    this._closed = true;
    if (this._inputWriter) {
      this._inputWriter.close();
      this._inputWriter = null;
    } else {
      const emptySchema = new Schema([]);
      const ipc = serializeIpcStream(emptySchema, []);
      this._writeFn(ipc);
    }
    const drainPromise = (async () => {
      try {
        if (!this._outputStreamOpened) {
          const schema2 = await this._reader.openNextStream();
          if (schema2) {
            while (await this._reader.readNextBatch() !== null) {}
          }
        } else {
          while (await this._reader.readNextBatch() !== null) {}
        }
      } catch {} finally {
        this._releaseBusy();
      }
    })();
    this._setDrainPromise(drainPromise);
  }
}
function pipeConnect(readable, writable, options) {
  const onLog = options?.onLog;
  const externalConfig = options?.externalLocation;
  let reader = null;
  let readerPromise = null;
  let methodCache = null;
  let protocolName = "";
  let serverProtocolVersion = "";
  let _busy = false;
  let _drainPromise = null;
  let closed = false;
  const writeFn = (bytes) => {
    let offset = 0;
    do {
      const end = Math.min(offset + MAX_STREAM_CHUNK, bytes.length);
      writable.write(bytes.subarray(offset, end));
      offset = end;
    } while (offset < bytes.length);
    writable.flush?.();
  };
  async function ensureReader() {
    if (reader)
      return reader;
    if (!readerPromise) {
      readerPromise = IpcStreamReader.create(readable);
    }
    reader = await readerPromise;
    return reader;
  }
  async function acquireBusy() {
    if (_drainPromise) {
      await _drainPromise;
      _drainPromise = null;
    }
    if (_busy) {
      throw new Error("Pipe transport is busy — another call or stream is in progress. " + "Pipe connections are single-threaded; wait for the current operation to complete.");
    }
    _busy = true;
  }
  function releaseBusy() {
    _busy = false;
  }
  function setDrainPromise(p) {
    _drainPromise = p;
  }
  async function ensureMethodCache() {
    if (methodCache)
      return methodCache;
    await acquireBusy();
    try {
      const emptySchema = new Schema([]);
      const body = buildRequestIpc(emptySchema, {}, DESCRIBE_METHOD_NAME);
      writeFn(body);
      const r = await ensureReader();
      const response = await r.readStream();
      if (!response) {
        throw new Error("EOF reading __describe__ response");
      }
      const desc = await parseDescribeResponse(response.batches, onLog);
      protocolName = desc.protocolName;
      serverProtocolVersion = desc.protocolVersion;
      methodCache = new Map(desc.methods.map((m) => [m.name, m]));
      return methodCache;
    } finally {
      releaseBusy();
    }
  }
  return {
    async call(method, params) {
      const methods = await ensureMethodCache();
      await acquireBusy();
      try {
        const info = methods.get(method);
        if (!info) {
          throw new Error(`Unknown method: '${method}'`);
        }
        const r = await ensureReader();
        const fullParams = { ...info.defaults ?? {}, ...params ?? {} };
        const body = buildRequestIpc(info.paramsSchema, fullParams, method, { protocolVersion: serverProtocolVersion });
        writeFn(body);
        const response = await r.readStream();
        if (!response) {
          throw new Error("EOF reading response");
        }
        let resultBatch = null;
        for (let batch of response.batches) {
          if (batch.numRows === 0) {
            if (isExternalLocationBatch(batch)) {
              batch = await resolveExternalLocation(batch, externalConfig);
            } else {
              dispatchLogOrError(batch, onLog);
              continue;
            }
          }
          resultBatch = batch;
        }
        if (!resultBatch) {
          return null;
        }
        const rows = extractBatchRows(resultBatch);
        if (rows.length === 0)
          return null;
        if (info.resultSchema.fields.length === 0)
          return null;
        return rows[0];
      } finally {
        releaseBusy();
      }
    },
    async stream(method, params) {
      const methods = await ensureMethodCache();
      await acquireBusy();
      try {
        const info = methods.get(method);
        if (!info) {
          throw new Error(`Unknown method: '${method}'`);
        }
        const r = await ensureReader();
        const fullParams = { ...info.defaults ?? {}, ...params ?? {} };
        const body = buildRequestIpc(info.paramsSchema, fullParams, method, { protocolVersion: serverProtocolVersion });
        writeFn(body);
        let header = null;
        if (info.headerSchema) {
          const headerStream = await r.readStream();
          if (headerStream) {
            for (const batch of headerStream.batches) {
              if (batch.numRows === 0) {
                dispatchLogOrError(batch, onLog);
                continue;
              }
              const rows = extractBatchRows(batch);
              if (rows.length > 0) {
                header = rows[0];
              }
            }
          }
        }
        const outputSchema = info.outputSchema ?? info.resultSchema;
        return new PipeStreamSession({
          reader: r,
          writeFn,
          onLog,
          header,
          outputSchema,
          releaseBusy,
          setDrainPromise,
          externalConfig
        });
      } catch (e) {
        try {
          const r = await ensureReader();
          const emptySchema = new Schema([]);
          const ipc = serializeIpcStream(emptySchema, []);
          writeFn(ipc);
          const outStream = await r.readStream();
        } catch {}
        releaseBusy();
        throw e;
      }
    },
    async describe() {
      const methods = await ensureMethodCache();
      return {
        protocolName,
        protocolVersion: serverProtocolVersion,
        methods: [...methods.values()]
      };
    },
    close() {
      if (closed)
        return;
      closed = true;
      writable.end();
    }
  };
}
function subprocessConnect(cmd, options) {
  const proc = Bun.spawn(cmd, {
    stdin: "pipe",
    stdout: "pipe",
    stderr: options?.stderr ?? "ignore",
    cwd: options?.cwd,
    env: options?.env ? { ...process.env, ...options.env } : undefined
  });
  const stdout = proc.stdout;
  const writable = {
    write(data) {
      proc.stdin.write(data);
    },
    flush() {
      proc.stdin.flush();
    },
    end() {
      proc.stdin.end();
    }
  };
  const client = pipeConnect(stdout, writable, {
    onLog: options?.onLog,
    externalLocation: options?.externalLocation
  });
  const originalClose = client.close;
  client.close = () => {
    originalClose.call(client);
    try {
      proc.kill();
    } catch {}
  };
  return client;
}

// src/client/tcp.ts
function tcpConnect(host, port, options) {
  const socket = connect({ host, port });
  socket.setNoDelay(true);
  socket.on("error", () => {});
  const readable = socket;
  const writable = {
    write(data) {
      socket.write(data);
    },
    end() {
      socket.end();
    }
  };
  const client = pipeConnect(readable, writable, {
    onLog: options?.onLog,
    externalLocation: options?.externalLocation
  });
  const originalClose = client.close;
  client.close = () => {
    originalClose.call(client);
    try {
      socket.destroy();
    } catch {}
  };
  return client;
}
// src/access-log.ts
var _NODE_FS_MOD2 = "node:fs";
function _loadWriteSync2() {
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req) {
    throw new Error("FdSink requires Node.js or Bun (node:fs.writeSync). For other runtimes, " + "supply a custom AccessLogSink that wraps console.log or your logger.");
  }
  return req(_NODE_FS_MOD2).writeSync;
}

class FdSink {
  fd;
  _writeSync = _loadWriteSync2();
  constructor(fd) {
    this.fd = fd;
  }
  write(line) {
    const buf = new TextEncoder().encode(line);
    let offset = 0;
    while (offset < buf.length) {
      const n = this._writeSync(this.fd, buf, offset, buf.length - offset);
      if (n <= 0)
        throw new Error(`access-log writeSync returned ${n}`);
      offset += n;
    }
  }
}
function rfc3339Utc() {
  const d = new Date;
  const yyyy = d.getUTCFullYear().toString().padStart(4, "0");
  const mm = (d.getUTCMonth() + 1).toString().padStart(2, "0");
  const dd = d.getUTCDate().toString().padStart(2, "0");
  const hh = d.getUTCHours().toString().padStart(2, "0");
  const mi = d.getUTCMinutes().toString().padStart(2, "0");
  const ss = d.getUTCSeconds().toString().padStart(2, "0");
  const ms = d.getUTCMilliseconds().toString().padStart(3, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}.${ms}Z`;
}
function base64(bytes) {
  return Buffer.from(bytes).toString("base64");
}
function roundTo2(f) {
  return Math.round(f * 100) / 100;
}
var _ENCODER = new TextEncoder;
function utf8Length(value) {
  return _ENCODER.encode(value).byteLength;
}
var REDACTED = "[redacted]";
var CLAIM_REDACT_RE = /password|token|secret|key|authorization|email|phone|address|birthdate|gender|^name$|given_name|family_name|middle_name|nickname|preferred_username|picture|profile|website/i;
function redactClaims(claims) {
  const out = {};
  for (const [k, v] of Object.entries(claims)) {
    out[k] = CLAIM_REDACT_RE.test(k) ? REDACTED : v;
  }
  return out;
}
function noRedaction(claims) {
  return { ...claims };
}
var TRACE_ID_RE = /^[0-9a-f]{32}$/;
var SPAN_ID_RE = /^[0-9a-f]{16}$/;
var _OTEL_MOD = "@opentelemetry/api";
var _otelTrace = null;
function otelTraceContext() {
  if (_otelTrace === null) {
    _otelTrace = false;
    try {
      const req = import.meta.require ?? globalThis.require ?? null;
      if (req)
        _otelTrace = req(_OTEL_MOD)?.trace ?? false;
    } catch {
      _otelTrace = false;
    }
  }
  if (!_otelTrace)
    return null;
  try {
    const ctx = _otelTrace.getActiveSpan()?.spanContext();
    if (!ctx)
      return null;
    const traceId = String(ctx.traceId ?? "");
    const spanId = String(ctx.spanId ?? "");
    if (!TRACE_ID_RE.test(traceId) || !SPAN_ID_RE.test(spanId))
      return null;
    if (/^0+$/.test(traceId) || /^0+$/.test(spanId))
      return null;
    return { traceId, spanId };
  } catch {
    return null;
  }
}
function fnv1a32(text) {
  let hash = 2166136261;
  for (let i = 0;i < text.length; i++) {
    hash ^= text.charCodeAt(i) & 255;
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash >>> 0;
}

class AccessLogSampler {
  rate;
  threshold;
  constructor(rate) {
    this.rate = rate;
    if (!(typeof rate === "number" && Number.isFinite(rate) && rate >= 0 && rate <= 1)) {
      throw new RangeError(`access-log sample rate must be between 0.0 and 1.0, got ${rate}`);
    }
    this.threshold = rate * 4294967295;
  }
  keep(record, key) {
    if (this.rate >= 1)
      return true;
    if (record.status === "error")
      return true;
    if (fnv1a32(key) > this.threshold)
      return false;
    record.sample_rate = this.rate;
    return true;
  }
}

class AsyncRecordQueue {
  capacity;
  writeRecord;
  queue = [];
  dropped = 0;
  scheduled = false;
  constructor(capacity, writeRecord) {
    this.capacity = capacity;
    this.writeRecord = writeRecord;
  }
  enqueue(record) {
    if (this.queue.length >= this.capacity) {
      this.dropped++;
      return;
    }
    if (this.dropped) {
      record.dropped_records = this.dropped;
      this.dropped = 0;
    }
    this.queue.push(record);
    if (!this.scheduled) {
      this.scheduled = true;
      setTimeout(() => {
        this.scheduled = false;
        this.flush();
      }, 0);
    }
  }
  flush() {
    while (this.queue.length > 0) {
      const record = this.queue.shift();
      this.writeRecord(record);
    }
  }
  get droppedCount() {
    return this.dropped;
  }
}

class AccessLogHook {
  sink;
  serverVersion;
  level;
  maxRecordBytes;
  sampler;
  traceContext;
  redactor;
  queue;
  constructor(sink, options = {}) {
    this.sink = sink;
    const opts = typeof options === "string" ? { serverVersion: options } : options;
    this.serverVersion = opts.serverVersion ?? "";
    this.level = opts.level ?? "INFO";
    this.maxRecordBytes = opts.maxRecordBytes ?? 1048576;
    this.sampler = new AccessLogSampler(opts.sampleRate ?? 1);
    this.traceContext = opts.traceContext ?? otelTraceContext;
    this.redactor = opts.redactor ?? redactClaims;
    this.queue = opts.async ? new AsyncRecordQueue(opts.queueSize ?? 1e4, (rec) => this.write(rec)) : null;
  }
  flush() {
    this.queue?.flush();
  }
  get droppedRecords() {
    return this.queue?.droppedCount ?? 0;
  }
  onDispatchStart(_info) {
    const token = { startNs: process.hrtime.bigint() };
    return token;
  }
  onDispatchEnd(token, info, stats, error) {
    const t = token;
    const durationMs = t ? roundTo2(Number(process.hrtime.bigint() - t.startNs) / 1e6) : 0;
    const status = error ? "error" : "ok";
    const errType = error ? error.type ?? error.constructor.name : "";
    const errMsg = error?.message ?? "";
    const protocol = info.protocol ?? "";
    const rec = {
      timestamp: rfc3339Utc(),
      level: "INFO",
      logger: "vgi_rpc.access",
      message: `${protocol}.${info.method} ${status}`,
      server_id: info.serverId,
      protocol,
      protocol_hash: info.protocolHash ?? "",
      method: info.method,
      method_type: info.methodType,
      principal: info.principal ?? "",
      auth_domain: info.authDomain ?? "",
      authenticated: info.authenticated ?? false,
      remote_addr: info.remoteAddr ?? "",
      duration_ms: durationMs,
      status,
      error_type: errType
    };
    if (errMsg)
      rec.error_message = errMsg;
    if (this.serverVersion)
      rec.server_version = this.serverVersion;
    if (info.protocolVersion)
      rec.protocol_version = info.protocolVersion;
    if (info.requestId)
      rec.request_id = info.requestId;
    if (info.httpStatus !== undefined)
      rec.http_status = info.httpStatus;
    const trace = this.currentTrace();
    if (trace) {
      rec.trace_id = trace.traceId;
      rec.span_id = trace.spanId;
    }
    if (info.requestData && info.requestData.length > 0) {
      const encoded = base64(info.requestData);
      if (this.level === "DEBUG") {
        rec.request_data = encoded;
      } else {
        rec.original_request_bytes = encoded.length;
        rec.truncated = "payload_omitted";
      }
    }
    if (info.methodType === "stream") {
      rec.stream_id = info.streamId ?? "00000000000000000000000000000000";
    }
    if (info.cancelled)
      rec.cancelled = true;
    if (info.claims && Object.keys(info.claims).length > 0) {
      const claims = this.redact(info.claims);
      if (Object.keys(claims).length > 0)
        rec.claims = claims;
    }
    if (info.requestBytes !== undefined)
      rec.request_bytes = info.requestBytes;
    if (info.externalizedBytes)
      rec.externalized_bytes = info.externalizedBytes;
    if (stats.inputBatches + stats.outputBatches + stats.inputRows + stats.outputRows + stats.inputBytes + stats.outputBytes !== 0) {
      rec.input_batches = stats.inputBatches;
      rec.output_batches = stats.outputBatches;
      rec.input_rows = stats.inputRows;
      rec.output_rows = stats.outputRows;
      rec.input_bytes = stats.inputBytes;
      rec.output_bytes = stats.outputBytes;
    }
    const sampleKey = info.streamId || info.requestId || `${rec.timestamp}:${info.method}`;
    if (info.deferral) {
      info.deferral.defer((responseBytes) => {
        if (responseBytes !== undefined)
          rec.response_bytes = responseBytes;
        this.emit(rec, sampleKey);
      });
      return;
    }
    this.emit(rec, sampleKey);
  }
  redact(claims) {
    try {
      return this.redactor(claims);
    } catch (err2) {
      console.warn("vgi-rpc access log: claim redactor threw; dropping claims from the record", err2);
      return {};
    }
  }
  currentTrace() {
    try {
      return this.traceContext();
    } catch {
      return null;
    }
  }
  emit(rec, sampleKey) {
    if (!this.sampler.keep(rec, sampleKey))
      return;
    if (this.queue) {
      this.queue.enqueue(rec);
      return;
    }
    this.write(rec);
  }
  write(rec) {
    try {
      this.sink.write(`${this.format(rec)}
`);
    } catch {}
  }
  format(rec) {
    let line = JSON.stringify(rec);
    if (this.maxRecordBytes <= 0 || utf8Length(line) <= this.maxRecordBytes)
      return line;
    const requestData = rec.request_data;
    if (typeof requestData === "string") {
      rec.original_request_bytes = requestData.length;
      delete rec.request_data;
      rec.truncated = true;
      line = JSON.stringify(rec);
      if (utf8Length(line) <= this.maxRecordBytes)
        return line;
    }
    if (rec.claims !== undefined) {
      rec.claims = {};
      rec.truncated = true;
      line = JSON.stringify(rec);
      if (utf8Length(line) <= this.maxRecordBytes)
        return line;
    }
    const sentinel = {
      timestamp: rec.timestamp,
      level: "INFO",
      logger: "vgi_rpc.access",
      message: "record_too_large",
      server_id: rec.server_id ?? "",
      protocol: rec.protocol ?? "",
      protocol_hash: rec.protocol_hash ?? "",
      method: rec.method ?? "",
      method_type: rec.method_type ?? "unary",
      principal: rec.principal ?? "",
      auth_domain: rec.auth_domain ?? "",
      authenticated: rec.authenticated ?? false,
      remote_addr: rec.remote_addr ?? "",
      duration_ms: rec.duration_ms ?? 0,
      status: rec.status ?? "ok",
      error_type: rec.error_type ?? "",
      truncated: "record_too_large"
    };
    if (rec.method_type === "stream" && typeof rec.stream_id === "string")
      sentinel.stream_id = rec.stream_id;
    if (sentinel.status === "error") {
      sentinel.error_message = typeof rec.error_message === "string" && rec.error_message ? rec.error_message : "record_too_large";
    }
    if (typeof rec.dropped_records === "number")
      sentinel.dropped_records = rec.dropped_records;
    if (typeof rec.sample_rate === "number")
      sentinel.sample_rate = rec.sample_rate;
    return JSON.stringify(sentinel);
  }
}
// src/auth.ts
class AuthContext {
  domain;
  authenticated;
  principal;
  claims;
  constructor(domain, authenticated, principal, claims = {}) {
    this.domain = domain;
    this.authenticated = authenticated;
    this.principal = principal;
    this.claims = claims;
  }
  static anonymous() {
    return new AuthContext("", false, null);
  }
  requireAuthenticated() {
    if (!this.authenticated) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
  }
}
// src/client/connect.ts
import { Schema as Schema4 } from "@query-farm/apache-arrow";

// src/client/capabilities.ts
var MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes";
var UPLOAD_URL_HEADER = "VGI-Upload-URL-Support";
var MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes";
function parseHeaderInt(headers, name) {
  const raw = headers.get(name) ?? headers.get(name.toLowerCase());
  if (raw == null)
    return null;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}
function parseCapabilitiesFromHeaders(headers) {
  const uploadRaw = headers.get(UPLOAD_URL_HEADER) ?? headers.get(UPLOAD_URL_HEADER.toLowerCase());
  const uploadUrlSupport = uploadRaw === "true";
  let cacheExpiresAt = null;
  const cc = headers.get("Cache-Control") ?? headers.get("cache-control");
  if (cc) {
    for (const token of cc.split(",")) {
      const t = token.trim().toLowerCase();
      if (t.startsWith("max-age=")) {
        const seconds = Number.parseFloat(t.slice("max-age=".length));
        if (Number.isFinite(seconds)) {
          cacheExpiresAt = Date.now() + seconds * 1000;
        }
        break;
      }
    }
  }
  return {
    maxRequestBytes: parseHeaderInt(headers, MAX_REQUEST_BYTES_HEADER),
    uploadUrlSupport,
    maxUploadBytes: parseHeaderInt(headers, MAX_UPLOAD_BYTES_HEADER),
    cacheExpiresAt
  };
}
function isCapabilitySnapshotFresh(snapshot) {
  if (!snapshot)
    return false;
  if (snapshot.cacheExpiresAt == null)
    return true;
  return Date.now() < snapshot.cacheExpiresAt;
}

// src/client/stream.ts
import { Field as Field2, makeData as makeData2, RecordBatch as RecordBatch2, Schema as Schema2, Struct as Struct2, vectorFromArray as vectorFromArray2 } from "@query-farm/apache-arrow";
function packResumeToken(cursor, callToken) {
  return callToken === null ? cursor : `${cursor.length}:${cursor}${callToken}`;
}
function unpackResumeToken(token) {
  const sep = token.indexOf(":");
  if (sep < 0)
    return { cursor: token, callToken: null };
  const cursorLen = Number(token.slice(0, sep));
  if (!Number.isInteger(cursorLen) || cursorLen < 0)
    return { cursor: token, callToken: null };
  const rest = token.slice(sep + 1);
  if (cursorLen > rest.length)
    return { cursor: token, callToken: null };
  const call = rest.slice(cursorLen);
  return { cursor: rest.slice(0, cursorLen), callToken: call === "" ? null : call };
}

class HttpStreamSession {
  _baseUrl;
  _prefix;
  _method;
  _stateToken;
  _callStateToken;
  _outputSchema;
  _inputSchema;
  _onLog;
  _pendingBatches;
  _finished;
  _header;
  _compressionLevel;
  _compressFn;
  _decompressFn;
  _authorization;
  _externalConfig;
  _postFn;
  constructor(opts) {
    this._baseUrl = opts.baseUrl;
    this._prefix = opts.prefix;
    this._method = opts.method;
    this._stateToken = opts.stateToken;
    this._callStateToken = opts.callStateToken ?? null;
    this._outputSchema = opts.outputSchema;
    this._inputSchema = opts.inputSchema;
    this._onLog = opts.onLog;
    this._pendingBatches = opts.pendingBatches;
    this._finished = opts.finished;
    this._header = opts.header;
    this._compressionLevel = opts.compressionLevel;
    this._compressFn = opts.compressFn;
    this._decompressFn = opts.decompressFn;
    this._authorization = opts.authorization;
    this._externalConfig = opts.externalConfig;
    this._postFn = opts.postFn;
  }
  async _post(url, body) {
    if (this._postFn)
      return this._postFn(url, body);
    return fetch(url, {
      method: "POST",
      headers: this._buildHeaders(),
      body: await this._prepareBody(body)
    });
  }
  get header() {
    return this._header;
  }
  _tokenMetadata(token) {
    const metadata = new Map;
    metadata.set(STATE_KEY, token);
    if (this._callStateToken !== null) {
      metadata.set(CALL_STATE_KEY, this._callStateToken);
    }
    return metadata;
  }
  _resumeToken() {
    return this._stateToken === null ? null : packResumeToken(this._stateToken, this._callStateToken);
  }
  _buildHeaders() {
    const headers = {
      "Content-Type": ARROW_CONTENT_TYPE
    };
    if (this._compressionLevel != null && this._compressFn) {
      headers["Content-Encoding"] = "zstd";
    }
    if (this._compressionLevel != null && this._decompressFn) {
      headers["Accept-Encoding"] = "zstd";
    }
    if (this._authorization) {
      headers.Authorization = this._authorization;
    }
    return headers;
  }
  async _prepareBody(content) {
    if (this._compressionLevel != null && this._compressFn) {
      return await this._compressFn(content, this._compressionLevel);
    }
    return content;
  }
  async _readResponse(resp) {
    const body = new Uint8Array(await resp.arrayBuffer());
    return new Uint8Array(await decodeResponseBody(resp.headers, body, this._decompressFn));
  }
  async exchange(input) {
    if (this._stateToken === null) {
      throw new RpcError("ProtocolError", "Stream has finished — no state token available", "");
    }
    if (input.length === 0) {
      const zeroSchema = this._inputSchema ?? this._outputSchema;
      const emptyBatch = this._buildEmptyBatch(zeroSchema);
      const batchWithMeta = new RecordBatch2(zeroSchema, emptyBatch.data, this._tokenMetadata(this._stateToken));
      return this._doExchange(zeroSchema, [batchWithMeta]);
    }
    const keys = Object.keys(input[0]);
    const fields = keys.map((key) => {
      let sample;
      for (const row of input) {
        if (row[key] != null) {
          sample = row[key];
          break;
        }
      }
      const arrowType = inferArrowType(sample);
      const nullable = input.some((row) => row[key] == null);
      return new Field2(key, arrowType, nullable);
    });
    const inputSchema = new Schema2(fields);
    const children = inputSchema.fields.map((f) => {
      const values = input.map((row) => row[f.name]);
      return vectorFromArray2(values, f.type).data[0];
    });
    const structType = new Struct2(inputSchema.fields);
    const data = makeData2({
      type: structType,
      length: input.length,
      children,
      nullCount: 0
    });
    const batch = new RecordBatch2(inputSchema, data, this._tokenMetadata(this._stateToken));
    return this._doExchange(inputSchema, [batch]);
  }
  async _doExchange(schema2, batches) {
    const body = serializeIpcStream(schema2, batches);
    const resp = await this._post(`${this._baseUrl}${this._prefix}/${this._method}/exchange`, body);
    if (resp.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
    const responseBody = await this._readResponse(resp);
    const { batches: responseBatches } = await readResponseBatches(responseBody);
    let resultRows = [];
    for (const batch of responseBatches) {
      if (batch.numRows === 0) {
        dispatchLogOrError(batch, this._onLog);
        const token2 = batch.metadata?.get(STATE_KEY);
        if (token2) {
          this._stateToken = token2;
        }
        continue;
      }
      const token = batch.metadata?.get(STATE_KEY);
      if (token) {
        this._stateToken = token;
      }
      resultRows = extractBatchRows(batch);
    }
    return resultRows;
  }
  _buildEmptyBatch(schema2) {
    const children = schema2.fields.map((f) => {
      return makeData2({ type: f.type, length: 0, nullCount: 0 });
    });
    const structType = new Struct2(schema2.fields);
    const data = makeData2({
      type: structType,
      length: 0,
      children,
      nullCount: 0
    });
    return new RecordBatch2(schema2, data);
  }
  async* [Symbol.asyncIterator]() {
    for (let batch of this._pendingBatches) {
      if (batch.numRows === 0) {
        if (isExternalLocationBatch(batch)) {
          batch = await resolveExternalLocation(batch, this._externalConfig);
        } else {
          dispatchLogOrError(batch, this._onLog);
          continue;
        }
      }
      yield extractBatchRows(batch);
    }
    this._pendingBatches = [];
    if (this._finished)
      return;
    if (this._stateToken === null)
      return;
    while (true) {
      const stateToken = this._stateToken;
      if (stateToken === null)
        return;
      const responseBody = await this._sendContinuation(stateToken);
      const { batches } = await readResponseBatches(responseBody);
      let gotContinuation = false;
      for (let batch of batches) {
        if (batch.numRows === 0) {
          const token = batch.metadata?.get(STATE_KEY);
          if (token) {
            this._stateToken = token;
            gotContinuation = true;
            continue;
          }
          if (isExternalLocationBatch(batch)) {
            batch = await resolveExternalLocation(batch, this._externalConfig);
          } else {
            dispatchLogOrError(batch, this._onLog);
            continue;
          }
        }
        yield extractBatchRows(batch);
      }
      if (!gotContinuation)
        break;
    }
  }
  async nextWithToken() {
    const multi = "nextWithToken requires one data batch per response; the upstream " + "worker buffered multiple (configured max_response_bytes?)";
    while (this._pendingBatches.length > 0) {
      let batch = this._pendingBatches.shift();
      if (batch.numRows === 0) {
        if (isExternalLocationBatch(batch)) {
          batch = await resolveExternalLocation(batch, this._externalConfig);
        } else {
          dispatchLogOrError(batch, this._onLog);
          continue;
        }
      }
      if (this._pendingBatches.some((b) => b.numRows > 0 || isExternalLocationBatch(b))) {
        throw new Error(multi);
      }
      return { rows: extractBatchRows(batch), token: this._resumeToken() };
    }
    if (this._finished || this._stateToken === null) {
      this._finished = true;
      return null;
    }
    const responseBody = await this._sendContinuation(this._stateToken);
    const { batches } = await readResponseBatches(responseBody);
    let dataRows = null;
    let nextToken = null;
    for (let batch of batches) {
      if (batch.numRows === 0) {
        const token = batch.metadata?.get(STATE_KEY);
        if (token) {
          nextToken = token;
          continue;
        }
        if (isExternalLocationBatch(batch)) {
          batch = await resolveExternalLocation(batch, this._externalConfig);
        } else {
          dispatchLogOrError(batch, this._onLog);
          continue;
        }
      }
      if (dataRows !== null) {
        throw new Error(multi);
      }
      dataRows = extractBatchRows(batch);
    }
    this._stateToken = nextToken;
    if (dataRows === null) {
      this._finished = true;
      return null;
    }
    return { rows: dataRows, token: this._resumeToken() };
  }
  seekToToken(token) {
    const { cursor, callToken } = unpackResumeToken(token);
    this._pendingBatches = [];
    this._stateToken = cursor;
    this._callStateToken = callToken;
    this._finished = false;
  }
  async _sendContinuation(token) {
    const emptySchema = new Schema2([]);
    const metadata = this._tokenMetadata(token);
    const structType = new Struct2(emptySchema.fields);
    const data = makeData2({
      type: structType,
      length: 1,
      children: [],
      nullCount: 0
    });
    const batch = new RecordBatch2(emptySchema, data, metadata);
    const body = serializeIpcStream(emptySchema, [batch]);
    const resp = await this._post(`${this._baseUrl}${this._prefix}/${this._method}/exchange`, body);
    if (resp.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
    return this._readResponse(resp);
  }
  close() {}
}

// src/client/uploadUrl.ts
import { Field as Field3, Int64 as Int642, RecordBatchReader as RecordBatchReader4, Schema as Schema3 } from "@query-farm/apache-arrow";
var UPLOAD_URL_METHOD2 = "__upload_url__";
var UPLOAD_URL_PARAMS_SCHEMA2 = new Schema3([new Field3("count", new Int642, false)]);
async function requestUploadUrls(baseUrl, prefix, count, authorization) {
  const body = buildRequestIpc(UPLOAD_URL_PARAMS_SCHEMA2, { count: BigInt(count) }, UPLOAD_URL_METHOD2);
  const headers = { "Content-Type": ARROW_CONTENT_TYPE };
  if (authorization)
    headers.Authorization = authorization;
  const resp = await fetch(`${baseUrl}${prefix}/${UPLOAD_URL_METHOD2}/init`, {
    method: "POST",
    headers,
    body
  });
  if (resp.status === 404) {
    throw new RpcError("NotSupported", "Server does not support upload URLs", "");
  }
  if (resp.status === 401) {
    throw new RpcError("AuthenticationError", "Authentication required", "");
  }
  if (!resp.ok) {
    throw new RpcError("HttpError", `__upload_url__/init failed: HTTP ${resp.status}`, "");
  }
  const respBody = new Uint8Array(await resp.arrayBuffer());
  const reader = await RecordBatchReader4.from(respBody);
  await reader.open();
  const pairs = [];
  for (const batch of reader.readAll()) {
    if (batch.numRows === 0)
      continue;
    for (let r = 0;r < batch.numRows; r++) {
      const uploadUrl = batch.getChildAt(0)?.get(r);
      const downloadUrl = batch.getChildAt(1)?.get(r);
      const expiresRaw = batch.getChildAt(2)?.get(r);
      let expiresAt;
      if (expiresRaw instanceof Date) {
        expiresAt = expiresRaw;
      } else if (typeof expiresRaw === "bigint") {
        expiresAt = new Date(Number(expiresRaw / 1000n));
      } else if (typeof expiresRaw === "number") {
        expiresAt = new Date(expiresRaw);
      } else {
        expiresAt = new Date;
      }
      pairs.push({ uploadUrl, downloadUrl, expiresAt });
    }
  }
  if (pairs.length === 0) {
    throw new RpcError("ProtocolError", "Server returned no upload URLs", "");
  }
  return pairs;
}
async function buildPointerRequestBody(originalBody, downloadUrl) {
  const reader = await RecordBatchReader4.from(originalBody);
  await reader.open();
  const schema2 = reader.schema;
  if (!schema2) {
    throw new RpcError("ProtocolError", "Original request body has no schema", "");
  }
  const batches = reader.readAll();
  if (batches.length === 0) {
    throw new RpcError("ProtocolError", "Original request body has no batches", "");
  }
  const original = batches[0];
  const originalMeta = original.metadata ?? new Map;
  const pointer = makeExternalLocationBatch(schema2, downloadUrl);
  const merged = new Map(pointer.metadata ?? new Map);
  const method = originalMeta.get(RPC_METHOD_KEY);
  const version = originalMeta.get(REQUEST_VERSION_KEY) ?? REQUEST_VERSION;
  if (method)
    merged.set(RPC_METHOD_KEY, method);
  merged.set(REQUEST_VERSION_KEY, version);
  for (const [k, v] of originalMeta) {
    if (!merged.has(k))
      merged.set(k, v);
  }
  const { RecordBatch: RecordBatch3 } = await import("@query-farm/apache-arrow");
  const pointerWithMeta = new RecordBatch3(schema2, pointer.data, merged);
  return serializeIpcStream(schema2, [pointerWithMeta]);
}
async function externalizeRequestBody(body, opts) {
  const pairs = await requestUploadUrls(opts.baseUrl, opts.prefix, 1, opts.authorization);
  const pair = pairs[0];
  if (opts.urlValidator) {
    opts.urlValidator(pair.uploadUrl);
    opts.urlValidator(pair.downloadUrl);
  }
  const putResp = await fetch(pair.uploadUrl, {
    method: "PUT",
    headers: { "Content-Type": ARROW_CONTENT_TYPE },
    body
  });
  if (!putResp.ok) {
    throw new RpcError("ExternalUploadFailed", `PUT to upload URL failed: HTTP ${putResp.status}`, "");
  }
  return buildPointerRequestBody(body, pair.downloadUrl);
}

// src/client/connect.ts
function httpConnect(baseUrl, options) {
  const prefix = (options?.prefix ?? "").replace(/\/+$/, "");
  const onLog = options?.onLog;
  const compressionLevel = options?.compressionLevel;
  const authorization = options?.authorization;
  const externalConfig = options?.externalLocation;
  let methodCache = null;
  let serverProtocolVersion = "";
  let compressFn;
  let decompressFn;
  let compressionLoaded = false;
  let capabilities = null;
  function updateCapabilitiesFromResponse(resp) {
    const next = parseCapabilitiesFromHeaders(resp.headers);
    if (next.maxRequestBytes != null || next.uploadUrlSupport) {
      capabilities = next;
    }
  }
  async function maybeExternalize(body) {
    const caps = isCapabilitySnapshotFresh(capabilities) ? capabilities : null;
    if (!caps)
      return body;
    if (!caps.uploadUrlSupport)
      return body;
    if (caps.maxRequestBytes == null || body.byteLength <= caps.maxRequestBytes)
      return body;
    return externalizeRequestBody(body, {
      baseUrl,
      prefix,
      authorization,
      urlValidator: externalConfig?.urlValidator ?? null
    });
  }
  async function postWithExternalization(url, body) {
    const sendBody = await maybeExternalize(body);
    let resp = await fetch(url, {
      method: "POST",
      headers: buildHeaders(),
      body: await prepareBody(sendBody)
    });
    updateCapabilitiesFromResponse(resp);
    if (resp.status === 413 && capabilities?.uploadUrlSupport && body.byteLength > 0) {
      const externalized = await externalizeRequestBody(body, {
        baseUrl,
        prefix,
        authorization,
        urlValidator: externalConfig?.urlValidator ?? null
      });
      resp = await fetch(url, {
        method: "POST",
        headers: buildHeaders(),
        body: await prepareBody(externalized)
      });
      updateCapabilitiesFromResponse(resp);
    }
    return resp;
  }
  async function ensureCompression() {
    if (compressionLoaded || compressionLevel == null)
      return;
    try {
      const mod = await Promise.resolve().then(() => (init_zstd(), exports_zstd));
      compressFn = mod.zstdCompress;
      decompressFn = mod.zstdDecompress;
    } catch {}
    compressionLoaded = true;
  }
  function buildHeaders() {
    const headers = {
      "Content-Type": ARROW_CONTENT_TYPE
    };
    if (compressionLevel != null && compressFn) {
      headers["Content-Encoding"] = "zstd";
    }
    if (compressionLevel != null && decompressFn) {
      headers["Accept-Encoding"] = "zstd";
    }
    if (authorization) {
      headers.Authorization = authorization;
    }
    return headers;
  }
  async function prepareBody(content) {
    if (compressionLevel != null && compressFn) {
      return await compressFn(content, compressionLevel);
    }
    return content;
  }
  function checkAuth(resp) {
    if (resp.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
  }
  async function readResponse(resp) {
    const body = new Uint8Array(await resp.arrayBuffer());
    return new Uint8Array(await decodeResponseBody(resp.headers, body, decompressFn));
  }
  async function ensureMethodCache() {
    if (methodCache)
      return methodCache;
    await ensureCompression();
    const desc = await httpIntrospect(baseUrl, {
      prefix,
      authorization,
      compressionLevel,
      compressFn,
      decompressFn
    });
    methodCache = new Map(desc.methods.map((m) => [m.name, m]));
    serverProtocolVersion = desc.protocolVersion;
    return methodCache;
  }
  return {
    async call(method, params) {
      await ensureCompression();
      const methods = await ensureMethodCache();
      const info = methods.get(method);
      if (!info) {
        throw new Error(`Unknown method: '${method}'`);
      }
      const fullParams = { ...info.defaults ?? {}, ...params ?? {} };
      const body = buildRequestIpc(info.paramsSchema, fullParams, method, { protocolVersion: serverProtocolVersion });
      const resp = await postWithExternalization(`${baseUrl}${prefix}/${method}`, body);
      checkAuth(resp);
      const responseBody = await readResponse(resp);
      const { batches } = await readResponseBatches(responseBody);
      let resultBatch = null;
      for (let batch of batches) {
        if (batch.numRows === 0) {
          if (isExternalLocationBatch(batch)) {
            batch = await resolveExternalLocation(batch, externalConfig);
          } else {
            dispatchLogOrError(batch, onLog);
            continue;
          }
        }
        resultBatch = batch;
      }
      if (!resultBatch) {
        return null;
      }
      const rows = extractBatchRows(resultBatch);
      if (rows.length === 0)
        return null;
      const result = rows[0];
      if (info.resultSchema.fields.length === 0)
        return null;
      return result;
    },
    async stream(method, params) {
      await ensureCompression();
      const methods = await ensureMethodCache();
      const info = methods.get(method);
      if (!info) {
        throw new Error(`Unknown method: '${method}'`);
      }
      const fullParams = { ...info.defaults ?? {}, ...params ?? {} };
      const body = buildRequestIpc(info.paramsSchema, fullParams, method, { protocolVersion: serverProtocolVersion });
      const resp = await postWithExternalization(`${baseUrl}${prefix}/${method}/init`, body);
      checkAuth(resp);
      const responseBody = await readResponse(resp);
      let header = null;
      let stateToken = null;
      let callStateToken = null;
      const pendingBatches = [];
      let finished = false;
      let streamSchema = null;
      if (info.headerSchema) {
        const reader = await readSequentialStreams(responseBody);
        const headerStream = await reader.readStream();
        if (headerStream) {
          for (const batch of headerStream.batches) {
            if (batch.numRows === 0) {
              dispatchLogOrError(batch, onLog);
              continue;
            }
            const rows = extractBatchRows(batch);
            if (rows.length > 0) {
              header = rows[0];
            }
          }
        }
        const dataStream = await reader.readStream();
        if (dataStream) {
          streamSchema = dataStream.schema;
        }
        const headerErrorBatches = [];
        if (dataStream) {
          for (const batch of dataStream.batches) {
            if (batch.numRows === 0) {
              const token = batch.metadata?.get(STATE_KEY);
              if (token) {
                stateToken = token;
                callStateToken = batch.metadata?.get(CALL_STATE_KEY) ?? callStateToken;
                continue;
              }
              const level = batch.metadata?.get(LOG_LEVEL_KEY);
              if (level === "EXCEPTION") {
                headerErrorBatches.push(batch);
                continue;
              }
              dispatchLogOrError(batch, onLog);
              continue;
            }
            pendingBatches.push(batch);
          }
        }
        if (headerErrorBatches.length > 0) {
          if (pendingBatches.length > 0 || stateToken !== null) {
            pendingBatches.push(...headerErrorBatches);
          } else {
            for (const batch of headerErrorBatches) {
              dispatchLogOrError(batch, onLog);
            }
          }
        }
        if (!dataStream && !stateToken) {
          finished = true;
        }
      } else {
        const { schema: responseSchema, batches } = await readResponseBatches(responseBody);
        streamSchema = responseSchema;
        const errorBatches = [];
        for (const batch of batches) {
          if (batch.numRows === 0) {
            const token = batch.metadata?.get(STATE_KEY);
            if (token) {
              stateToken = token;
              callStateToken = batch.metadata?.get(CALL_STATE_KEY) ?? callStateToken;
              continue;
            }
            const level = batch.metadata?.get(LOG_LEVEL_KEY);
            if (level === "EXCEPTION") {
              errorBatches.push(batch);
              continue;
            }
            dispatchLogOrError(batch, onLog);
            continue;
          }
          pendingBatches.push(batch);
        }
        if (errorBatches.length > 0) {
          if (pendingBatches.length > 0 || stateToken !== null) {
            pendingBatches.push(...errorBatches);
          } else {
            for (const batch of errorBatches) {
              dispatchLogOrError(batch, onLog);
            }
          }
        }
      }
      if (pendingBatches.length === 0 && stateToken === null) {
        finished = true;
      }
      const outputSchema = (streamSchema && streamSchema.fields.length > 0 ? streamSchema : null) ?? (pendingBatches.length > 0 ? pendingBatches[0].schema : null) ?? info.outputSchema ?? info.resultSchema;
      return new HttpStreamSession({
        baseUrl,
        prefix,
        method,
        stateToken,
        callStateToken,
        outputSchema,
        inputSchema: info.inputSchema,
        onLog,
        pendingBatches,
        finished,
        header,
        compressionLevel,
        compressFn,
        decompressFn,
        authorization,
        externalConfig,
        postFn: postWithExternalization
      });
    },
    async resumeStream(method, token, outputSchema) {
      await ensureCompression();
      const { cursor, callToken } = unpackResumeToken(token);
      return new HttpStreamSession({
        baseUrl,
        prefix,
        method,
        stateToken: cursor,
        callStateToken: callToken,
        outputSchema: outputSchema ?? new Schema4([]),
        onLog,
        pendingBatches: [],
        finished: false,
        header: null,
        compressionLevel,
        compressFn,
        decompressFn,
        authorization,
        externalConfig,
        postFn: postWithExternalization
      });
    },
    async describe() {
      await ensureCompression();
      return httpIntrospect(baseUrl, {
        prefix,
        authorization,
        compressionLevel,
        compressFn,
        decompressFn
      });
    },
    close() {}
  };
}
// src/client/oauth.ts
function parseMetadataJson(json) {
  const result = {
    resource: json.resource,
    authorizationServers: json.authorization_servers
  };
  if (json.scopes_supported)
    result.scopesSupported = json.scopes_supported;
  if (json.bearer_methods_supported)
    result.bearerMethodsSupported = json.bearer_methods_supported;
  if (json.resource_signing_alg_values_supported)
    result.resourceSigningAlgValuesSupported = json.resource_signing_alg_values_supported;
  if (json.resource_name)
    result.resourceName = json.resource_name;
  if (json.resource_documentation)
    result.resourceDocumentation = json.resource_documentation;
  if (json.resource_policy_uri)
    result.resourcePolicyUri = json.resource_policy_uri;
  if (json.resource_tos_uri)
    result.resourceTosUri = json.resource_tos_uri;
  if (json.client_id)
    result.clientId = json.client_id;
  if (json.client_secret)
    result.clientSecret = json.client_secret;
  if (json.use_id_token_as_bearer)
    result.useIdTokenAsBearer = json.use_id_token_as_bearer;
  if (json.device_code_client_id)
    result.deviceCodeClientId = json.device_code_client_id;
  if (json.device_code_client_secret)
    result.deviceCodeClientSecret = json.device_code_client_secret;
  return result;
}
async function httpOAuthMetadata(baseUrl, prefix) {
  const effectivePrefix = (prefix ?? "").replace(/\/+$/, "");
  const metadataUrl = `${baseUrl.replace(/\/+$/, "")}/.well-known/oauth-protected-resource${effectivePrefix}`;
  try {
    return await fetchOAuthMetadata(metadataUrl);
  } catch {
    return null;
  }
}
async function fetchOAuthMetadata(metadataUrl) {
  const response = await fetch(metadataUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch OAuth metadata from ${metadataUrl}: ${response.status}`);
  }
  const json = await response.json();
  return parseMetadataJson(json);
}
function parseResourceMetadataUrl(wwwAuthenticate) {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch)
    return null;
  const params = bearerMatch[1];
  const metadataMatch = params.match(/resource_metadata="([^"]+)"/);
  if (!metadataMatch)
    return null;
  return metadataMatch[1];
}
function parseClientId(wwwAuthenticate) {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch)
    return null;
  const params = bearerMatch[1];
  const clientIdMatch = params.match(/client_id="([^"]+)"/);
  if (!clientIdMatch)
    return null;
  return clientIdMatch[1];
}
function parseClientSecret(wwwAuthenticate) {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch)
    return null;
  const params = bearerMatch[1];
  const match = params.match(/client_secret="([^"]+)"/);
  if (!match)
    return null;
  return match[1];
}
function parseUseIdTokenAsBearer(wwwAuthenticate) {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch)
    return false;
  const params = bearerMatch[1];
  const match = params.match(/use_id_token_as_bearer="([^"]+)"/);
  if (!match)
    return false;
  return match[1] === "true";
}
function parseDeviceCodeClientId(wwwAuthenticate) {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch)
    return null;
  const params = bearerMatch[1];
  const match = params.match(/device_code_client_id="([^"]+)"/);
  if (!match)
    return null;
  return match[1];
}
function parseDeviceCodeClientSecret(wwwAuthenticate) {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch)
    return null;
  const params = bearerMatch[1];
  const match = params.match(/device_code_client_secret="([^"]+)"/);
  if (!match)
    return null;
  return match[1];
}
// src/http/auth.ts
function oauthResourceMetadataToJson(metadata) {
  const json = {
    resource: metadata.resource,
    authorization_servers: metadata.authorizationServers
  };
  if (metadata.scopesSupported)
    json.scopes_supported = metadata.scopesSupported;
  if (metadata.bearerMethodsSupported)
    json.bearer_methods_supported = metadata.bearerMethodsSupported;
  if (metadata.resourceSigningAlgValuesSupported)
    json.resource_signing_alg_values_supported = metadata.resourceSigningAlgValuesSupported;
  if (metadata.resourceName)
    json.resource_name = metadata.resourceName;
  if (metadata.resourceDocumentation)
    json.resource_documentation = metadata.resourceDocumentation;
  if (metadata.resourcePolicyUri)
    json.resource_policy_uri = metadata.resourcePolicyUri;
  if (metadata.resourceTosUri)
    json.resource_tos_uri = metadata.resourceTosUri;
  if (metadata.clientId) {
    if (!/^[A-Za-z0-9\-._~]+$/.test(metadata.clientId)) {
      throw new Error(`Invalid client_id: must contain only URL-safe characters [A-Za-z0-9\\-._~]`);
    }
    json.client_id = metadata.clientId;
  }
  if (metadata.clientSecret) {
    if (!/^[A-Za-z0-9\-._~]+$/.test(metadata.clientSecret)) {
      throw new Error(`Invalid client_secret: must contain only URL-safe characters [A-Za-z0-9\\-._~]`);
    }
    json.client_secret = metadata.clientSecret;
  }
  if (metadata.deviceCodeClientId) {
    if (!/^[A-Za-z0-9\-._~]+$/.test(metadata.deviceCodeClientId)) {
      throw new Error(`Invalid device_code_client_id: must contain only URL-safe characters [A-Za-z0-9\\-._~]`);
    }
    json.device_code_client_id = metadata.deviceCodeClientId;
  }
  if (metadata.deviceCodeClientSecret) {
    if (!/^[A-Za-z0-9\-._~]+$/.test(metadata.deviceCodeClientSecret)) {
      throw new Error(`Invalid device_code_client_secret: must contain only URL-safe characters [A-Za-z0-9\\-._~]`);
    }
    json.device_code_client_secret = metadata.deviceCodeClientSecret;
  }
  if (metadata.useIdTokenAsBearer) {
    json.use_id_token_as_bearer = true;
  }
  return json;
}
function wellKnownPath(prefix) {
  return `/.well-known/oauth-protected-resource${prefix}`;
}
function buildWwwAuthenticateHeader(metadataUrl, clientId, clientSecret, useIdTokenAsBearer, deviceCodeClientId, deviceCodeClientSecret) {
  let header = "Bearer";
  if (metadataUrl) {
    header += ` resource_metadata="${metadataUrl}"`;
  }
  if (clientId) {
    header += `, client_id="${clientId}"`;
  }
  if (clientSecret) {
    header += `, client_secret="${clientSecret}"`;
  }
  if (deviceCodeClientId) {
    header += `, device_code_client_id="${deviceCodeClientId}"`;
  }
  if (deviceCodeClientSecret) {
    header += `, device_code_client_secret="${deviceCodeClientSecret}"`;
  }
  if (useIdTokenAsBearer) {
    header += `, use_id_token_as_bearer="true"`;
  }
  return header;
}
// src/util/web-crypto.ts
function randomBytes(length) {
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  return buf;
}
function constantTimeEqual(a, b) {
  if (a.length !== b.length)
    return false;
  let diff = 0;
  for (let i = 0;i < a.length; i++)
    diff |= a[i] ^ b[i];
  return diff === 0;
}
var _hmacKeyCache = new Map;
async function sha256(data) {
  const digest = await crypto.subtle.digest("SHA-256", data);
  return new Uint8Array(digest);
}
async function sha256Hex2(data) {
  const bytes = await sha256(data);
  let s = "";
  for (let i = 0;i < bytes.length; i++)
    s += bytes[i].toString(16).padStart(2, "0");
  return s;
}

// src/http/unauthorized.ts
var AUTH_REASON_HEADER = "VGI-Auth-Reason";
var AUTH_PROXY_REQUIRED_HEADER = "VGI-Auth-Proxy-Required";
var AuthReason = {
  MissingCredential: "missing_credential",
  InvalidCredential: "invalid_credential",
  ExpiredCredential: "expired_credential",
  InsufficientScope: "insufficient_scope",
  ProxyRequired: "proxy_required",
  Unauthorized: "unauthorized"
};
var AUTH_REASONS = new Set(Object.values(AuthReason));
var AUTH_REASON_PROPERTY = "vgiAuthReason";

class AuthFailure extends Error {
  reason;
  detail;
  vgiAuthReason;
  constructor(reason, detail = "") {
    super(detail || reason);
    this.name = "AuthFailure";
    this.reason = reason;
    this.detail = detail;
    this.vgiAuthReason = reason;
  }
}

class AuthUnavailableError extends Error {
  retryAfter;
  detail;
  constructor(detail = "", retryAfter = 5) {
    super(detail || "authentication service unavailable");
    this.name = "AuthUnavailableError";
    this.detail = detail;
    this.retryAfter = retryAfter;
  }
}
function classifyAuthFailure(err2) {
  const declared = err2?.[AUTH_REASON_PROPERTY];
  const message = err2 instanceof Error ? err2.message : "";
  if (typeof declared === "string" && AUTH_REASONS.has(declared)) {
    return { reason: declared, detail: err2 instanceof AuthFailure ? err2.detail : message };
  }
  if (err2 instanceof Error && err2.name === "PermissionError") {
    return { reason: AuthReason.InsufficientScope, detail: message };
  }
  return { reason: AuthReason.Unauthorized, detail: message };
}
function buildProxyHint(headers) {
  const names = [...new Set(headers)];
  if (names.length === 0)
    return "";
  return `This service only accepts requests that arrive through its configured reverse proxy, ` + `which must set the ${names.join(", ")} header(s). A rejection here is at least as likely ` + `to be a proxy misconfiguration as a bad credential — check that the proxy is forwarding ` + `them before re-issuing credentials.`;
}
function unauthorizedEnvelope(reason, detail, proxyHint = "") {
  const payload = { error: "unauthorized", reason, detail };
  if (proxyHint)
    payload.proxy_hint = proxyHint;
  return JSON.stringify(payload);
}

// src/http/bearer.ts
function bearerAuthenticate(options) {
  const { validate } = options;
  return async function authenticate(request) {
    const authHeader = request.headers.get("Authorization") ?? "";
    if (!authHeader) {
      throw new AuthFailure(AuthReason.MissingCredential, "Missing Authorization header");
    }
    if (!authHeader.startsWith("Bearer ")) {
      throw new AuthFailure(AuthReason.InvalidCredential, "Authorization header is not a Bearer credential");
    }
    const token = authHeader.slice(7);
    return validate(token);
  };
}
function safeEqual(a, b) {
  const enc = new TextEncoder;
  return constantTimeEqual(enc.encode(a), enc.encode(b));
}
function bearerAuthenticateStatic(options) {
  const entries = options.tokens instanceof Map ? [...options.tokens.entries()] : Object.entries(options.tokens);
  function validate(token) {
    for (const [key, ctx] of entries) {
      if (safeEqual(token, key))
        return ctx;
    }
    throw new AuthFailure(AuthReason.InvalidCredential, "Unknown bearer token");
  }
  return bearerAuthenticate({ validate });
}
function isCredentialError(err2) {
  if (err2 instanceof AuthUnavailableError)
    return false;
  if (err2 instanceof AuthFailure)
    return true;
  return err2 instanceof Error && err2.constructor === Error && err2.name !== "PermissionError";
}
function combineReasons(codes) {
  if (codes.length === 0)
    return AuthReason.Unauthorized;
  const substantive = codes.find((code) => code !== AuthReason.MissingCredential);
  return substantive ?? AuthReason.MissingCredential;
}
function chainAuthenticate(...authenticators) {
  if (authenticators.length === 0) {
    throw new Error("chainAuthenticate requires at least one authenticator");
  }
  return async function authenticate(request) {
    let lastError = null;
    const codes = [];
    for (const authFn of authenticators) {
      try {
        return await authFn(request);
      } catch (err2) {
        if (isCredentialError(err2)) {
          lastError = err2;
          codes.push(err2 instanceof AuthFailure ? err2.reason : AuthReason.Unauthorized);
          continue;
        }
        throw err2;
      }
    }
    const error = new AuthFailure(combineReasons(codes), "No authenticator accepted the request");
    if (lastError)
      error.cause = lastError;
    throw error;
  };
}
// src/util/schema.ts
function serializeSchema2(schema2) {
  return serializeSchema(schema2);
}

// src/dispatch/describe.ts
var DESCRIBE_SCHEMA = schema([
  field("name", utf8(), false),
  field("method_type", utf8(), false),
  field("has_return", bool(), false),
  field("params_schema_ipc", binary(), false),
  field("result_schema_ipc", binary(), false),
  field("has_header", bool(), false),
  field("header_schema_ipc", binary(), true),
  field("is_exchange", bool(), true)
]);
async function computeProtocolHash(protocolName, rows) {
  const enc = new TextEncoder;
  const parts = [];
  const push = (v) => parts.push(typeof v === "string" ? enc.encode(v) : v);
  push("vgi_rpc.describe.v");
  push(DESCRIBE_VERSION);
  push("|");
  push(REQUEST_VERSION);
  push("|");
  push(protocolName);
  push("|");
  for (const r of rows) {
    push(Uint8Array.of(31));
    push(r.name);
    push(Uint8Array.of(30));
    push(r.methodType);
    push(Uint8Array.of(30));
    push(r.hasReturn ? "1" : "0");
    push(Uint8Array.of(30));
    push(r.hasHeader ? "1" : "0");
    push(Uint8Array.of(30));
    push(r.isExchange === null ? "-" : r.isExchange ? "1" : "0");
    push(Uint8Array.of(30));
    push(r.paramsIpc);
    push(Uint8Array.of(30));
    push(r.resultIpc);
    push(Uint8Array.of(30));
    if (r.headerIpc)
      push(r.headerIpc);
  }
  let total = 0;
  for (const p of parts)
    total += p.length;
  const buf = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    buf.set(p, off);
    off += p.length;
  }
  return sha256Hex2(buf);
}
async function buildDescribeBatch(protocolName, methods, serverId, protocolVersion) {
  const sortedEntries = [...methods.entries()].sort(([a], [b]) => a.localeCompare(b));
  const names = [];
  const methodTypes = [];
  const hasReturns = [];
  const paramsSchemas = [];
  const resultSchemas = [];
  const hasHeaders = [];
  const headerSchemas = [];
  const isExchanges = [];
  const hashRows = [];
  for (const [name, method] of sortedEntries) {
    names.push(name);
    methodTypes.push(method.type);
    const hasReturn = method.type === "unary" && method.resultSchema.fields.length > 0;
    hasReturns.push(hasReturn);
    const paramsIpc = serializeSchema2(method.paramsSchema);
    const resultIpc = serializeSchema2(method.resultSchema);
    paramsSchemas.push(paramsIpc);
    resultSchemas.push(resultIpc);
    const hasHeader = !!method.headerSchema;
    hasHeaders.push(hasHeader);
    const headerIpc = method.headerSchema ? serializeSchema2(method.headerSchema) : null;
    headerSchemas.push(headerIpc);
    const isExchange = null;
    isExchanges.push(isExchange);
    hashRows.push({
      name,
      methodType: method.type,
      hasReturn,
      hasHeader,
      isExchange,
      paramsIpc,
      resultIpc,
      headerIpc
    });
  }
  const baseBatch = batchFromColumns(DESCRIBE_SCHEMA, {
    name: names,
    method_type: methodTypes,
    has_return: hasReturns,
    params_schema_ipc: paramsSchemas,
    result_schema_ipc: resultSchemas,
    has_header: hasHeaders,
    header_schema_ipc: headerSchemas,
    is_exchange: isExchanges
  });
  const protocolHash = await computeProtocolHash(protocolName, hashRows);
  const metadata = new Map;
  metadata.set(PROTOCOL_NAME_KEY, protocolName);
  metadata.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  metadata.set(DESCRIBE_VERSION_KEY, DESCRIBE_VERSION);
  metadata.set(PROTOCOL_HASH_KEY, protocolHash);
  metadata.set(SERVER_ID_KEY, serverId);
  if (protocolVersion) {
    metadata.set(PROTOCOL_VERSION_KEY, protocolVersion);
  }
  const batch = withBatchMetadata(baseBatch, metadata);
  return { batch, metadata };
}

// src/types.ts
var MethodType;
((MethodType2) => {
  MethodType2["UNARY"] = "unary";
  MethodType2["STREAM"] = "stream";
})(MethodType ||= {});
var TransportKind;
((TransportKind2) => {
  TransportKind2["PIPE"] = "pipe";
  TransportKind2["HTTP"] = "http";
  TransportKind2["UNIX"] = "unix";
  TransportKind2["TCP"] = "tcp";
})(TransportKind ||= {});
var EMPTY_COOKIES = new Map;
function cookieNotUnaryHttpError() {
  return new Error("setCookie/deleteCookie is only supported inside unary RPC methods served over HTTP");
}

class RuntimeError extends Error {
  constructor(message) {
    super(message);
    this.name = "RuntimeError";
  }
}
function runtimeError(message) {
  return new RuntimeError(message);
}

class OutputCollector {
  _batches = [];
  _dataBatchIdx = null;
  _finished = false;
  _producerMode;
  _outputSchema;
  _serverId;
  _requestId;
  _cookieSinkEnabled = false;
  _responseCookies = [];
  _stickyContext = null;
  auth;
  cookies;
  kind;
  remainingResponseBytes;
  remainingExternalizedResponseBytes;
  externalizationEnabled;
  constructor(outputSchema, producerMode = true, serverId = "", requestId = null, authContext, cookies, kind, budgets) {
    this._outputSchema = outputSchema;
    this._producerMode = producerMode;
    this._serverId = serverId;
    this._requestId = requestId;
    this.auth = authContext ?? AuthContext.anonymous();
    this.cookies = cookies ?? EMPTY_COOKIES;
    this.kind = kind;
    this.remainingResponseBytes = budgets?.remainingResponseBytes;
    this.remainingExternalizedResponseBytes = budgets?.remainingExternalizedResponseBytes;
    this.externalizationEnabled = budgets?.externalizationEnabled;
  }
  enableCookieSink() {
    this._cookieSinkEnabled = true;
  }
  drainResponseCookies() {
    const cookies = this._responseCookies;
    this._responseCookies = [];
    return cookies;
  }
  setCookie(name, value, attrs) {
    if (!this._cookieSinkEnabled)
      throw cookieNotUnaryHttpError();
    this._responseCookies.push({
      name,
      value,
      delete: false,
      ...attrs ?? {}
    });
  }
  deleteCookie(name, opts) {
    if (!this._cookieSinkEnabled)
      throw cookieNotUnaryHttpError();
    this._responseCookies.push({
      name,
      value: "",
      delete: true,
      path: opts?.path,
      domain: opts?.domain
    });
  }
  attachStickyContext(ctx) {
    this._stickyContext = ctx;
  }
  get session() {
    return this._stickyContext?.state ?? null;
  }
  get sessionId() {
    return this._stickyContext?.sessionId ?? null;
  }
  openSession(state, ttl) {
    const sink = this._stickyContext;
    if (!sink) {
      throw runtimeError("sticky sessions not available on this transport");
    }
    if (!sink.acceptOpens) {
      throw runtimeError("client did not opt in to sticky sessions " + "(missing VGI-Session-Accept: true header — open the call inside " + "an HttpConnection.with_session_token() block)");
    }
    if (sink.state !== null) {
      throw runtimeError("a sticky session is already active for this request");
    }
    sink._open(state, ttl);
    sink.action = "open";
  }
  closeSession() {
    const sink = this._stickyContext;
    if (!sink) {
      throw runtimeError("sticky sessions not available on this transport");
    }
    sink._close();
    sink.action = "close";
  }
  get outputSchema() {
    return this._outputSchema;
  }
  get finished() {
    return this._finished;
  }
  get batches() {
    return this._batches;
  }
  get dataBatchIdx() {
    return this._dataBatchIdx;
  }
  emit(batchOrColumns, metadata) {
    let batch;
    if (isBatch(batchOrColumns)) {
      batch = batchOrColumns;
    } else {
      const coerced = coerceInt64(this._outputSchema, batchOrColumns);
      const cols = {};
      for (const f of this._outputSchema.fields) {
        const v = coerced[f.name];
        cols[f.name] = Array.isArray(v) ? v : [v];
      }
      batch = batchFromColumns(this._outputSchema, cols);
    }
    if (this._dataBatchIdx !== null) {
      throw new Error("Only one data batch may be emitted per call");
    }
    this._dataBatchIdx = this._batches.length;
    this._batches.push({ batch, metadata });
  }
  emitRow(values) {
    const columns = {};
    for (const [key, value] of Object.entries(values)) {
      columns[key] = [value];
    }
    this.emit(columns);
  }
  finish() {
    if (!this._producerMode) {
      throw new Error("finish() is not allowed on exchange streams; " + "exchange streams must emit exactly one data batch per call");
    }
    this._finished = true;
  }
  clientLog(level, message, extra) {
    const batch = buildLogBatch(this._outputSchema, level, message, extra, this._serverId, this._requestId);
    this._batches.push({ batch });
  }
}

// src/util/runtime.ts
function isWorkerd() {
  return typeof navigator !== "undefined" && navigator.userAgent === "Cloudflare-Workers";
}

// src/http/handler.ts
init_zstd();

// src/wire/opaque.ts
function isOpaquePassthroughType(type) {
  return isDate(type) || isTime(type) || isTimestamp(type) || isDuration(type) || isDecimal(type) || isLargeUtf8(type) || isLargeBinary(type) || isFixedSizeBinary(type) || isDictionary(type);
}

// src/wire/request.ts
var MIN_SAFE_BIG = BigInt(Number.MIN_SAFE_INTEGER);
var MAX_SAFE_BIG = BigInt(Number.MAX_SAFE_INTEGER);
function parseRequest(schema2, batch) {
  const metadata = batch.metadata ?? new Map;
  const methodName = metadata.get(RPC_METHOD_KEY);
  if (methodName === undefined) {
    throw new RpcError("ProtocolError", "Missing 'vgi_rpc.method' in request batch custom_metadata. " + "Each request batch must carry a 'vgi_rpc.method' key in its Arrow IPC custom_metadata " + "with the method name as a UTF-8 string.", "");
  }
  const version = metadata.get(REQUEST_VERSION_KEY);
  if (version === undefined) {
    throw new VersionError("Missing 'vgi_rpc.request_version' in request batch custom_metadata. " + `Set the 'vgi_rpc.request_version' custom_metadata value to '${REQUEST_VERSION}'.`);
  }
  if (version !== REQUEST_VERSION) {
    throw new VersionError(`Unsupported request version '${version}', expected '${REQUEST_VERSION}'. ` + `Set the 'vgi_rpc.request_version' custom_metadata value to '${REQUEST_VERSION}'.`);
  }
  const requestId = metadata.get(REQUEST_ID_KEY) ?? null;
  const params = {};
  if (schema2.fields.length > 0 && batch.numRows !== 1) {
    throw new RpcError("ProtocolError", `Expected 1 row in request batch, got ${batch.numRows}. ` + "Each parameter is a column (not a row). The batch should have exactly 1 row.", "");
  }
  const useOpaquePassthrough = backend.opaquePassthrough;
  for (let i = 0;i < schema2.fields.length; i++) {
    const field2 = schema2.fields[i];
    if (useOpaquePassthrough && (isMap(field2.type) || isOpaquePassthroughType(field2.type))) {
      const col = batch.getChildAt(i);
      params[field2.name] = col.data?.[0] ?? col.get(0);
      continue;
    }
    let value = batch.getChildAt(i)?.get(0);
    if (value instanceof Uint32Array && isOpaquePassthroughType(field2.type)) {
      try {
        value = BigInt(value.toString());
      } catch {}
    }
    if (typeof value === "bigint" && !isOpaquePassthroughType(field2.type)) {
      if (value >= MIN_SAFE_BIG && value <= MAX_SAFE_BIG) {
        value = Number(value);
      }
    }
    params[field2.name] = value;
  }
  return {
    methodName,
    requestVersion: version,
    requestId,
    schema: schema2,
    params,
    rawMetadata: metadata
  };
}
function applyDefaults(params, defaults) {
  if (!defaults)
    return params;
  for (const key of Object.keys(defaults)) {
    if (params[key] == null) {
      params[key] = defaults[key];
    }
  }
  return params;
}

// node_modules/@noble/ciphers/utils.js
/*! noble-ciphers - MIT License (c) 2023 Paul Miller (paulmillr.com) */
function isBytes(a) {
  return a instanceof Uint8Array || ArrayBuffer.isView(a) && a.constructor.name === "Uint8Array" && "BYTES_PER_ELEMENT" in a && a.BYTES_PER_ELEMENT === 1;
}
function abool(b) {
  if (typeof b !== "boolean")
    throw new TypeError(`boolean expected, not ${b}`);
}
function anumber(n) {
  if (typeof n !== "number")
    throw new TypeError("number expected, got " + typeof n);
  if (!Number.isSafeInteger(n) || n < 0)
    throw new RangeError("positive integer expected, got " + n);
}
function abytes(value, length, title = "") {
  const bytes = isBytes(value);
  const len = value?.length;
  const needsLen = length !== undefined;
  if (!bytes || needsLen && len !== length) {
    const prefix = title && `"${title}" `;
    const ofLen = needsLen ? ` of length ${length}` : "";
    const got = bytes ? `length=${len}` : `type=${typeof value}`;
    const message = prefix + "expected Uint8Array" + ofLen + ", got " + got;
    if (!bytes)
      throw new TypeError(message);
    throw new RangeError(message);
  }
  return value;
}
function aexists(instance, checkFinished = true) {
  if (instance.destroyed)
    throw new Error("Hash instance has been destroyed");
  if (checkFinished && instance.finished)
    throw new Error("Hash#digest() has already been called");
}
function aoutput(out, instance, onlyAligned = false) {
  abytes(out, undefined, "output");
  const min = instance.outputLen;
  if (out.length < min) {
    throw new RangeError("digestInto() expects output buffer of length at least " + min);
  }
  if (onlyAligned && !isAligned32(out))
    throw new Error("invalid output, must be aligned");
}
function u32(arr) {
  return new Uint32Array(arr.buffer, arr.byteOffset, Math.floor(arr.byteLength / 4));
}
function clean(...arrays) {
  for (let i = 0;i < arrays.length; i++) {
    arrays[i].fill(0);
  }
}
function createView(arr) {
  return new DataView(arr.buffer, arr.byteOffset, arr.byteLength);
}
var isLE = /* @__PURE__ */ (() => new Uint8Array(new Uint32Array([287454020]).buffer)[0] === 68)();
var byteSwap = (word) => word << 24 & 4278190080 | word << 8 & 16711680 | word >>> 8 & 65280 | word >>> 24 & 255;
var swap8IfBE = isLE ? (n) => n : (n) => byteSwap(n) >>> 0;
var byteSwap32 = (arr) => {
  for (let i = 0;i < arr.length; i++)
    arr[i] = byteSwap(arr[i]);
  return arr;
};
var swap32IfBE = isLE ? (u) => u : byteSwap32;
function checkOpts(defaults, opts) {
  if (opts == null || typeof opts !== "object")
    throw new Error("options must be defined");
  const merged = Object.assign(defaults, opts);
  return merged;
}
function equalBytes(a, b) {
  if (a.length !== b.length)
    return false;
  let diff = 0;
  for (let i = 0;i < a.length; i++)
    diff |= a[i] ^ b[i];
  return diff === 0;
}
function wrapMacConstructor(keyLen, macCons, fromMsg) {
  const mac = macCons;
  const getArgs = fromMsg || (() => []);
  const macC = (msg, key) => mac(key, ...getArgs(msg)).update(msg).digest();
  const tmp = mac(new Uint8Array(keyLen), ...getArgs(new Uint8Array(0)));
  macC.outputLen = tmp.outputLen;
  macC.blockLen = tmp.blockLen;
  macC.create = (key, ...args) => mac(key, ...args);
  return macC;
}
var wrapCipher = (params, constructor) => {
  function wrappedCipher(key, ...args) {
    abytes(key, undefined, "key");
    if (params.nonceLength !== undefined) {
      const nonce = args[0];
      abytes(nonce, params.varSizeNonce ? undefined : params.nonceLength, "nonce");
    }
    const tagl = params.tagLength;
    if (tagl && args[1] !== undefined)
      abytes(args[1], undefined, "AAD");
    const cipher = constructor(key, ...args);
    const checkOutput = (fnLength, output) => {
      if (output !== undefined) {
        if (fnLength !== 2)
          throw new Error("cipher output not supported");
        abytes(output, undefined, "output");
      }
    };
    let called = false;
    const wrCipher = {
      encrypt(data, output) {
        if (called)
          throw new Error("cannot encrypt() twice with same key + nonce");
        called = true;
        abytes(data);
        checkOutput(cipher.encrypt.length, output);
        return cipher.encrypt(data, output);
      },
      decrypt(data, output) {
        abytes(data);
        if (tagl && data.length < tagl)
          throw new Error('"ciphertext" expected length bigger than tagLength=' + tagl);
        checkOutput(cipher.decrypt.length, output);
        return cipher.decrypt(data, output);
      }
    };
    return wrCipher;
  }
  Object.assign(wrappedCipher, params);
  return wrappedCipher;
};
function getOutput(expectedLength, out, onlyAligned = true) {
  if (out === undefined)
    return new Uint8Array(expectedLength);
  abytes(out, undefined, "output");
  if (out.length !== expectedLength)
    throw new Error('"output" expected Uint8Array of length ' + expectedLength + ", got: " + out.length);
  if (onlyAligned && !isAligned32(out))
    throw new Error("invalid output, must be aligned");
  return out;
}
function u64Lengths(dataLength, aadLength, isLE2) {
  anumber(dataLength);
  anumber(aadLength);
  abool(isLE2);
  const num = new Uint8Array(16);
  const view = createView(num);
  view.setBigUint64(0, BigInt(aadLength), isLE2);
  view.setBigUint64(8, BigInt(dataLength), isLE2);
  return num;
}
function isAligned32(bytes) {
  return bytes.byteOffset % 4 === 0;
}
function copyBytes(bytes) {
  return Uint8Array.from(abytes(bytes));
}

// node_modules/@noble/ciphers/_arx.js
var encodeStr = (str) => Uint8Array.from(str.split(""), (c) => c.charCodeAt(0));
var sigma16_32 = /* @__PURE__ */ (() => swap32IfBE(u32(encodeStr("expand 16-byte k"))))();
var sigma32_32 = /* @__PURE__ */ (() => swap32IfBE(u32(encodeStr("expand 32-byte k"))))();
function rotl(a, b) {
  return a << b | a >>> 32 - b;
}
var BLOCK_LEN = 64;
var BLOCK_LEN32 = 16;
var MAX_COUNTER = /* @__PURE__ */ (() => 2 ** 32 - 1)();
var U32_EMPTY = /* @__PURE__ */ Uint32Array.of();
function runCipher(core, sigma, key, nonce, data, output, counter, rounds) {
  const len = data.length;
  const block = new Uint8Array(BLOCK_LEN);
  const b32 = u32(block);
  const isAligned = isLE && isAligned32(data) && isAligned32(output);
  const d32 = isAligned ? u32(data) : U32_EMPTY;
  const o32 = isAligned ? u32(output) : U32_EMPTY;
  if (!isLE) {
    for (let pos = 0;pos < len; counter++) {
      core(sigma, key, nonce, b32, counter, rounds);
      swap32IfBE(b32);
      if (counter >= MAX_COUNTER)
        throw new Error("arx: counter overflow");
      const take = Math.min(BLOCK_LEN, len - pos);
      for (let j = 0, posj;j < take; j++) {
        posj = pos + j;
        output[posj] = data[posj] ^ block[j];
      }
      pos += take;
    }
    return;
  }
  for (let pos = 0;pos < len; counter++) {
    core(sigma, key, nonce, b32, counter, rounds);
    if (counter >= MAX_COUNTER)
      throw new Error("arx: counter overflow");
    const take = Math.min(BLOCK_LEN, len - pos);
    if (isAligned && take === BLOCK_LEN) {
      const pos32 = pos / 4;
      if (pos % 4 !== 0)
        throw new Error("arx: invalid block position");
      for (let j = 0, posj;j < BLOCK_LEN32; j++) {
        posj = pos32 + j;
        o32[posj] = d32[posj] ^ b32[j];
      }
      pos += BLOCK_LEN;
      continue;
    }
    for (let j = 0, posj;j < take; j++) {
      posj = pos + j;
      output[posj] = data[posj] ^ block[j];
    }
    pos += take;
  }
}
function createCipher(core, opts) {
  const { allowShortKeys, extendNonceFn, counterLength, counterRight, rounds } = checkOpts({ allowShortKeys: false, counterLength: 8, counterRight: false, rounds: 20 }, opts);
  if (typeof core !== "function")
    throw new Error("core must be a function");
  anumber(counterLength);
  anumber(rounds);
  abool(counterRight);
  abool(allowShortKeys);
  return (key, nonce, data, output, counter = 0) => {
    abytes(key, undefined, "key");
    abytes(nonce, undefined, "nonce");
    abytes(data, undefined, "data");
    const len = data.length;
    output = getOutput(len, output, false);
    anumber(counter);
    if (counter < 0 || counter >= MAX_COUNTER)
      throw new Error("arx: counter overflow");
    const toClean = [];
    let l = key.length;
    let k;
    let sigma;
    if (l === 32) {
      toClean.push(k = copyBytes(key));
      sigma = sigma32_32;
    } else if (l === 16 && allowShortKeys) {
      k = new Uint8Array(32);
      k.set(key);
      k.set(key, 16);
      sigma = sigma16_32;
      toClean.push(k);
    } else {
      abytes(key, 32, "arx key");
      throw new Error("invalid key size");
    }
    if (!isLE || !isAligned32(nonce))
      toClean.push(nonce = copyBytes(nonce));
    let k32 = u32(k);
    if (extendNonceFn) {
      if (nonce.length !== 24)
        throw new Error(`arx: extended nonce must be 24 bytes`);
      const n16 = nonce.subarray(0, 16);
      if (isLE)
        extendNonceFn(sigma, k32, u32(n16), k32);
      else {
        const sigmaRaw = swap32IfBE(Uint32Array.from(sigma));
        extendNonceFn(sigmaRaw, k32, u32(n16), k32);
        clean(sigmaRaw);
        swap32IfBE(k32);
      }
      nonce = nonce.subarray(16);
    } else if (!isLE)
      swap32IfBE(k32);
    const nonceNcLen = 16 - counterLength;
    if (nonceNcLen !== nonce.length)
      throw new Error(`arx: nonce must be ${nonceNcLen} or 16 bytes`);
    if (nonceNcLen !== 12) {
      const nc = new Uint8Array(12);
      nc.set(nonce, counterRight ? 0 : 12 - nonce.length);
      nonce = nc;
      toClean.push(nonce);
    }
    const n32 = swap32IfBE(u32(nonce));
    try {
      runCipher(core, sigma, k32, n32, data, output, counter, rounds);
      return output;
    } finally {
      clean(...toClean);
    }
  };
}

// node_modules/@noble/ciphers/_poly1305.js
function u8to16(a, i) {
  return a[i++] & 255 | (a[i++] & 255) << 8;
}
class Poly1305 {
  blockLen = 16;
  outputLen = 16;
  buffer = new Uint8Array(16);
  r = new Uint16Array(10);
  h = new Uint16Array(10);
  pad = new Uint16Array(8);
  pos = 0;
  finished = false;
  destroyed = false;
  constructor(key) {
    key = copyBytes(abytes(key, 32, "key"));
    const t0 = u8to16(key, 0);
    const t1 = u8to16(key, 2);
    const t2 = u8to16(key, 4);
    const t3 = u8to16(key, 6);
    const t4 = u8to16(key, 8);
    const t5 = u8to16(key, 10);
    const t6 = u8to16(key, 12);
    const t7 = u8to16(key, 14);
    this.r[0] = t0 & 8191;
    this.r[1] = (t0 >>> 13 | t1 << 3) & 8191;
    this.r[2] = (t1 >>> 10 | t2 << 6) & 7939;
    this.r[3] = (t2 >>> 7 | t3 << 9) & 8191;
    this.r[4] = (t3 >>> 4 | t4 << 12) & 255;
    this.r[5] = t4 >>> 1 & 8190;
    this.r[6] = (t4 >>> 14 | t5 << 2) & 8191;
    this.r[7] = (t5 >>> 11 | t6 << 5) & 8065;
    this.r[8] = (t6 >>> 8 | t7 << 8) & 8191;
    this.r[9] = t7 >>> 5 & 127;
    for (let i = 0;i < 8; i++)
      this.pad[i] = u8to16(key, 16 + 2 * i);
  }
  process(data, offset, isLast = false) {
    const hibit = isLast ? 0 : 1 << 11;
    const { h, r } = this;
    const r0 = r[0];
    const r1 = r[1];
    const r2 = r[2];
    const r3 = r[3];
    const r4 = r[4];
    const r5 = r[5];
    const r6 = r[6];
    const r7 = r[7];
    const r8 = r[8];
    const r9 = r[9];
    const t0 = u8to16(data, offset + 0);
    const t1 = u8to16(data, offset + 2);
    const t2 = u8to16(data, offset + 4);
    const t3 = u8to16(data, offset + 6);
    const t4 = u8to16(data, offset + 8);
    const t5 = u8to16(data, offset + 10);
    const t6 = u8to16(data, offset + 12);
    const t7 = u8to16(data, offset + 14);
    let h0 = h[0] + (t0 & 8191);
    let h1 = h[1] + ((t0 >>> 13 | t1 << 3) & 8191);
    let h2 = h[2] + ((t1 >>> 10 | t2 << 6) & 8191);
    let h3 = h[3] + ((t2 >>> 7 | t3 << 9) & 8191);
    let h4 = h[4] + ((t3 >>> 4 | t4 << 12) & 8191);
    let h5 = h[5] + (t4 >>> 1 & 8191);
    let h6 = h[6] + ((t4 >>> 14 | t5 << 2) & 8191);
    let h7 = h[7] + ((t5 >>> 11 | t6 << 5) & 8191);
    let h8 = h[8] + ((t6 >>> 8 | t7 << 8) & 8191);
    let h9 = h[9] + (t7 >>> 5 | hibit);
    let c = 0;
    let d0 = c + h0 * r0 + h1 * (5 * r9) + h2 * (5 * r8) + h3 * (5 * r7) + h4 * (5 * r6);
    c = d0 >>> 13;
    d0 &= 8191;
    d0 += h5 * (5 * r5) + h6 * (5 * r4) + h7 * (5 * r3) + h8 * (5 * r2) + h9 * (5 * r1);
    c += d0 >>> 13;
    d0 &= 8191;
    let d1 = c + h0 * r1 + h1 * r0 + h2 * (5 * r9) + h3 * (5 * r8) + h4 * (5 * r7);
    c = d1 >>> 13;
    d1 &= 8191;
    d1 += h5 * (5 * r6) + h6 * (5 * r5) + h7 * (5 * r4) + h8 * (5 * r3) + h9 * (5 * r2);
    c += d1 >>> 13;
    d1 &= 8191;
    let d2 = c + h0 * r2 + h1 * r1 + h2 * r0 + h3 * (5 * r9) + h4 * (5 * r8);
    c = d2 >>> 13;
    d2 &= 8191;
    d2 += h5 * (5 * r7) + h6 * (5 * r6) + h7 * (5 * r5) + h8 * (5 * r4) + h9 * (5 * r3);
    c += d2 >>> 13;
    d2 &= 8191;
    let d3 = c + h0 * r3 + h1 * r2 + h2 * r1 + h3 * r0 + h4 * (5 * r9);
    c = d3 >>> 13;
    d3 &= 8191;
    d3 += h5 * (5 * r8) + h6 * (5 * r7) + h7 * (5 * r6) + h8 * (5 * r5) + h9 * (5 * r4);
    c += d3 >>> 13;
    d3 &= 8191;
    let d4 = c + h0 * r4 + h1 * r3 + h2 * r2 + h3 * r1 + h4 * r0;
    c = d4 >>> 13;
    d4 &= 8191;
    d4 += h5 * (5 * r9) + h6 * (5 * r8) + h7 * (5 * r7) + h8 * (5 * r6) + h9 * (5 * r5);
    c += d4 >>> 13;
    d4 &= 8191;
    let d5 = c + h0 * r5 + h1 * r4 + h2 * r3 + h3 * r2 + h4 * r1;
    c = d5 >>> 13;
    d5 &= 8191;
    d5 += h5 * r0 + h6 * (5 * r9) + h7 * (5 * r8) + h8 * (5 * r7) + h9 * (5 * r6);
    c += d5 >>> 13;
    d5 &= 8191;
    let d6 = c + h0 * r6 + h1 * r5 + h2 * r4 + h3 * r3 + h4 * r2;
    c = d6 >>> 13;
    d6 &= 8191;
    d6 += h5 * r1 + h6 * r0 + h7 * (5 * r9) + h8 * (5 * r8) + h9 * (5 * r7);
    c += d6 >>> 13;
    d6 &= 8191;
    let d7 = c + h0 * r7 + h1 * r6 + h2 * r5 + h3 * r4 + h4 * r3;
    c = d7 >>> 13;
    d7 &= 8191;
    d7 += h5 * r2 + h6 * r1 + h7 * r0 + h8 * (5 * r9) + h9 * (5 * r8);
    c += d7 >>> 13;
    d7 &= 8191;
    let d8 = c + h0 * r8 + h1 * r7 + h2 * r6 + h3 * r5 + h4 * r4;
    c = d8 >>> 13;
    d8 &= 8191;
    d8 += h5 * r3 + h6 * r2 + h7 * r1 + h8 * r0 + h9 * (5 * r9);
    c += d8 >>> 13;
    d8 &= 8191;
    let d9 = c + h0 * r9 + h1 * r8 + h2 * r7 + h3 * r6 + h4 * r5;
    c = d9 >>> 13;
    d9 &= 8191;
    d9 += h5 * r4 + h6 * r3 + h7 * r2 + h8 * r1 + h9 * r0;
    c += d9 >>> 13;
    d9 &= 8191;
    c = (c << 2) + c | 0;
    c = c + d0 | 0;
    d0 = c & 8191;
    c = c >>> 13;
    d1 += c;
    h[0] = d0;
    h[1] = d1;
    h[2] = d2;
    h[3] = d3;
    h[4] = d4;
    h[5] = d5;
    h[6] = d6;
    h[7] = d7;
    h[8] = d8;
    h[9] = d9;
  }
  finalize() {
    const { h, pad } = this;
    const g = new Uint16Array(10);
    let c = h[1] >>> 13;
    h[1] &= 8191;
    for (let i = 2;i < 10; i++) {
      h[i] += c;
      c = h[i] >>> 13;
      h[i] &= 8191;
    }
    h[0] += c * 5;
    c = h[0] >>> 13;
    h[0] &= 8191;
    h[1] += c;
    c = h[1] >>> 13;
    h[1] &= 8191;
    h[2] += c;
    g[0] = h[0] + 5;
    c = g[0] >>> 13;
    g[0] &= 8191;
    for (let i = 1;i < 10; i++) {
      g[i] = h[i] + c;
      c = g[i] >>> 13;
      g[i] &= 8191;
    }
    g[9] -= 1 << 13;
    let mask = (c ^ 1) - 1;
    for (let i = 0;i < 10; i++)
      g[i] &= mask;
    mask = ~mask;
    for (let i = 0;i < 10; i++)
      h[i] = h[i] & mask | g[i];
    h[0] = (h[0] | h[1] << 13) & 65535;
    h[1] = (h[1] >>> 3 | h[2] << 10) & 65535;
    h[2] = (h[2] >>> 6 | h[3] << 7) & 65535;
    h[3] = (h[3] >>> 9 | h[4] << 4) & 65535;
    h[4] = (h[4] >>> 12 | h[5] << 1 | h[6] << 14) & 65535;
    h[5] = (h[6] >>> 2 | h[7] << 11) & 65535;
    h[6] = (h[7] >>> 5 | h[8] << 8) & 65535;
    h[7] = (h[8] >>> 8 | h[9] << 5) & 65535;
    let f = h[0] + pad[0];
    h[0] = f & 65535;
    for (let i = 1;i < 8; i++) {
      f = (h[i] + pad[i] | 0) + (f >>> 16) | 0;
      h[i] = f & 65535;
    }
    clean(g);
  }
  update(data) {
    aexists(this);
    abytes(data);
    data = copyBytes(data);
    const { buffer, blockLen } = this;
    const len = data.length;
    for (let pos = 0;pos < len; ) {
      const take = Math.min(blockLen - this.pos, len - pos);
      if (take === blockLen) {
        for (;blockLen <= len - pos; pos += blockLen)
          this.process(data, pos);
        continue;
      }
      buffer.set(data.subarray(pos, pos + take), this.pos);
      this.pos += take;
      pos += take;
      if (this.pos === blockLen) {
        this.process(buffer, 0, false);
        this.pos = 0;
      }
    }
    return this;
  }
  destroy() {
    this.destroyed = true;
    clean(this.h, this.r, this.buffer, this.pad);
  }
  digestInto(out) {
    aexists(this);
    aoutput(out, this);
    this.finished = true;
    const { buffer, h } = this;
    let { pos } = this;
    if (pos) {
      buffer[pos++] = 1;
      for (;pos < 16; pos++)
        buffer[pos] = 0;
      this.process(buffer, 0, true);
    }
    this.finalize();
    let opos = 0;
    for (let i = 0;i < 8; i++) {
      out[opos++] = h[i] >>> 0;
      out[opos++] = h[i] >>> 8;
    }
  }
  digest() {
    const { buffer, outputLen } = this;
    this.digestInto(buffer);
    const res = buffer.slice(0, outputLen);
    this.destroy();
    return res;
  }
}
var poly1305 = /* @__PURE__ */ wrapMacConstructor(32, (key) => new Poly1305(key));

// node_modules/@noble/ciphers/chacha.js
function chachaCore(s, k, n, out, cnt, rounds = 20) {
  let y00 = s[0], y01 = s[1], y02 = s[2], y03 = s[3], y04 = k[0], y05 = k[1], y06 = k[2], y07 = k[3], y08 = k[4], y09 = k[5], y10 = k[6], y11 = k[7], y12 = cnt, y13 = n[0], y14 = n[1], y15 = n[2];
  let x00 = y00, x01 = y01, x02 = y02, x03 = y03, x04 = y04, x05 = y05, x06 = y06, x07 = y07, x08 = y08, x09 = y09, x10 = y10, x11 = y11, x12 = y12, x13 = y13, x14 = y14, x15 = y15;
  for (let r = 0;r < rounds; r += 2) {
    x00 = x00 + x04 | 0;
    x12 = rotl(x12 ^ x00, 16);
    x08 = x08 + x12 | 0;
    x04 = rotl(x04 ^ x08, 12);
    x00 = x00 + x04 | 0;
    x12 = rotl(x12 ^ x00, 8);
    x08 = x08 + x12 | 0;
    x04 = rotl(x04 ^ x08, 7);
    x01 = x01 + x05 | 0;
    x13 = rotl(x13 ^ x01, 16);
    x09 = x09 + x13 | 0;
    x05 = rotl(x05 ^ x09, 12);
    x01 = x01 + x05 | 0;
    x13 = rotl(x13 ^ x01, 8);
    x09 = x09 + x13 | 0;
    x05 = rotl(x05 ^ x09, 7);
    x02 = x02 + x06 | 0;
    x14 = rotl(x14 ^ x02, 16);
    x10 = x10 + x14 | 0;
    x06 = rotl(x06 ^ x10, 12);
    x02 = x02 + x06 | 0;
    x14 = rotl(x14 ^ x02, 8);
    x10 = x10 + x14 | 0;
    x06 = rotl(x06 ^ x10, 7);
    x03 = x03 + x07 | 0;
    x15 = rotl(x15 ^ x03, 16);
    x11 = x11 + x15 | 0;
    x07 = rotl(x07 ^ x11, 12);
    x03 = x03 + x07 | 0;
    x15 = rotl(x15 ^ x03, 8);
    x11 = x11 + x15 | 0;
    x07 = rotl(x07 ^ x11, 7);
    x00 = x00 + x05 | 0;
    x15 = rotl(x15 ^ x00, 16);
    x10 = x10 + x15 | 0;
    x05 = rotl(x05 ^ x10, 12);
    x00 = x00 + x05 | 0;
    x15 = rotl(x15 ^ x00, 8);
    x10 = x10 + x15 | 0;
    x05 = rotl(x05 ^ x10, 7);
    x01 = x01 + x06 | 0;
    x12 = rotl(x12 ^ x01, 16);
    x11 = x11 + x12 | 0;
    x06 = rotl(x06 ^ x11, 12);
    x01 = x01 + x06 | 0;
    x12 = rotl(x12 ^ x01, 8);
    x11 = x11 + x12 | 0;
    x06 = rotl(x06 ^ x11, 7);
    x02 = x02 + x07 | 0;
    x13 = rotl(x13 ^ x02, 16);
    x08 = x08 + x13 | 0;
    x07 = rotl(x07 ^ x08, 12);
    x02 = x02 + x07 | 0;
    x13 = rotl(x13 ^ x02, 8);
    x08 = x08 + x13 | 0;
    x07 = rotl(x07 ^ x08, 7);
    x03 = x03 + x04 | 0;
    x14 = rotl(x14 ^ x03, 16);
    x09 = x09 + x14 | 0;
    x04 = rotl(x04 ^ x09, 12);
    x03 = x03 + x04 | 0;
    x14 = rotl(x14 ^ x03, 8);
    x09 = x09 + x14 | 0;
    x04 = rotl(x04 ^ x09, 7);
  }
  let oi = 0;
  out[oi++] = y00 + x00 | 0;
  out[oi++] = y01 + x01 | 0;
  out[oi++] = y02 + x02 | 0;
  out[oi++] = y03 + x03 | 0;
  out[oi++] = y04 + x04 | 0;
  out[oi++] = y05 + x05 | 0;
  out[oi++] = y06 + x06 | 0;
  out[oi++] = y07 + x07 | 0;
  out[oi++] = y08 + x08 | 0;
  out[oi++] = y09 + x09 | 0;
  out[oi++] = y10 + x10 | 0;
  out[oi++] = y11 + x11 | 0;
  out[oi++] = y12 + x12 | 0;
  out[oi++] = y13 + x13 | 0;
  out[oi++] = y14 + x14 | 0;
  out[oi++] = y15 + x15 | 0;
}
function hchacha(s, k, i, out) {
  let x00 = swap8IfBE(s[0]), x01 = swap8IfBE(s[1]), x02 = swap8IfBE(s[2]), x03 = swap8IfBE(s[3]), x04 = swap8IfBE(k[0]), x05 = swap8IfBE(k[1]), x06 = swap8IfBE(k[2]), x07 = swap8IfBE(k[3]), x08 = swap8IfBE(k[4]), x09 = swap8IfBE(k[5]), x10 = swap8IfBE(k[6]), x11 = swap8IfBE(k[7]), x12 = swap8IfBE(i[0]), x13 = swap8IfBE(i[1]), x14 = swap8IfBE(i[2]), x15 = swap8IfBE(i[3]);
  for (let r = 0;r < 20; r += 2) {
    x00 = x00 + x04 | 0;
    x12 = rotl(x12 ^ x00, 16);
    x08 = x08 + x12 | 0;
    x04 = rotl(x04 ^ x08, 12);
    x00 = x00 + x04 | 0;
    x12 = rotl(x12 ^ x00, 8);
    x08 = x08 + x12 | 0;
    x04 = rotl(x04 ^ x08, 7);
    x01 = x01 + x05 | 0;
    x13 = rotl(x13 ^ x01, 16);
    x09 = x09 + x13 | 0;
    x05 = rotl(x05 ^ x09, 12);
    x01 = x01 + x05 | 0;
    x13 = rotl(x13 ^ x01, 8);
    x09 = x09 + x13 | 0;
    x05 = rotl(x05 ^ x09, 7);
    x02 = x02 + x06 | 0;
    x14 = rotl(x14 ^ x02, 16);
    x10 = x10 + x14 | 0;
    x06 = rotl(x06 ^ x10, 12);
    x02 = x02 + x06 | 0;
    x14 = rotl(x14 ^ x02, 8);
    x10 = x10 + x14 | 0;
    x06 = rotl(x06 ^ x10, 7);
    x03 = x03 + x07 | 0;
    x15 = rotl(x15 ^ x03, 16);
    x11 = x11 + x15 | 0;
    x07 = rotl(x07 ^ x11, 12);
    x03 = x03 + x07 | 0;
    x15 = rotl(x15 ^ x03, 8);
    x11 = x11 + x15 | 0;
    x07 = rotl(x07 ^ x11, 7);
    x00 = x00 + x05 | 0;
    x15 = rotl(x15 ^ x00, 16);
    x10 = x10 + x15 | 0;
    x05 = rotl(x05 ^ x10, 12);
    x00 = x00 + x05 | 0;
    x15 = rotl(x15 ^ x00, 8);
    x10 = x10 + x15 | 0;
    x05 = rotl(x05 ^ x10, 7);
    x01 = x01 + x06 | 0;
    x12 = rotl(x12 ^ x01, 16);
    x11 = x11 + x12 | 0;
    x06 = rotl(x06 ^ x11, 12);
    x01 = x01 + x06 | 0;
    x12 = rotl(x12 ^ x01, 8);
    x11 = x11 + x12 | 0;
    x06 = rotl(x06 ^ x11, 7);
    x02 = x02 + x07 | 0;
    x13 = rotl(x13 ^ x02, 16);
    x08 = x08 + x13 | 0;
    x07 = rotl(x07 ^ x08, 12);
    x02 = x02 + x07 | 0;
    x13 = rotl(x13 ^ x02, 8);
    x08 = x08 + x13 | 0;
    x07 = rotl(x07 ^ x08, 7);
    x03 = x03 + x04 | 0;
    x14 = rotl(x14 ^ x03, 16);
    x09 = x09 + x14 | 0;
    x04 = rotl(x04 ^ x09, 12);
    x03 = x03 + x04 | 0;
    x14 = rotl(x14 ^ x03, 8);
    x09 = x09 + x14 | 0;
    x04 = rotl(x04 ^ x09, 7);
  }
  let oi = 0;
  out[oi++] = x00;
  out[oi++] = x01;
  out[oi++] = x02;
  out[oi++] = x03;
  out[oi++] = x12;
  out[oi++] = x13;
  out[oi++] = x14;
  out[oi++] = x15;
  swap32IfBE(out);
}
var xchacha20 = /* @__PURE__ */ createCipher(chachaCore, {
  counterRight: false,
  counterLength: 8,
  extendNonceFn: hchacha,
  allowShortKeys: false
});
var ZEROS16 = /* @__PURE__ */ new Uint8Array(16);
var updatePadded = (h, msg) => {
  h.update(msg);
  const leftover = msg.length % 16;
  if (leftover)
    h.update(ZEROS16.subarray(leftover));
};
var ZEROS32 = /* @__PURE__ */ new Uint8Array(32);
function computeTag(fn, key, nonce, ciphertext, AAD) {
  if (AAD !== undefined)
    abytes(AAD, undefined, "AAD");
  const authKey = fn(key, nonce, ZEROS32);
  const lengths = u64Lengths(ciphertext.length, AAD ? AAD.length : 0, true);
  const h = poly1305.create(authKey);
  if (AAD)
    updatePadded(h, AAD);
  updatePadded(h, ciphertext);
  h.update(lengths);
  const res = h.digest();
  clean(authKey, lengths);
  return res;
}
var _poly1305_aead = (xorStream) => (key, nonce, AAD) => {
  const tagLength = 16;
  return {
    encrypt(plaintext, output) {
      const plength = plaintext.length;
      output = getOutput(plength + tagLength, output, false);
      output.set(plaintext);
      const oPlain = output.subarray(0, -tagLength);
      xorStream(key, nonce, oPlain, oPlain, 1);
      const tag = computeTag(xorStream, key, nonce, oPlain, AAD);
      output.set(tag, plength);
      clean(tag);
      return output;
    },
    decrypt(ciphertext, output) {
      output = getOutput(ciphertext.length - tagLength, output, false);
      const data = ciphertext.subarray(0, -tagLength);
      const passedTag = ciphertext.subarray(-tagLength);
      const tag = computeTag(xorStream, key, nonce, data, AAD);
      if (!equalBytes(passedTag, tag)) {
        clean(tag);
        throw new Error("invalid tag");
      }
      output.set(ciphertext.subarray(0, -tagLength));
      xorStream(key, nonce, output, output, 1);
      clean(tag);
      return output;
    }
  };
};
var xchacha20poly1305 = /* @__PURE__ */ wrapCipher({ blockSize: 64, nonceLength: 24, tagLength: 16 }, /* @__PURE__ */ _poly1305_aead(xchacha20));

// src/crypto.ts
var NONCE_LEN = 24;
var TAG_LEN = 16;
var VERSION_LEN = 1;
var MIN_ENVELOPE_LEN = VERSION_LEN + NONCE_LEN + TAG_LEN;

class SealError extends Error {
  constructor(message) {
    super(message);
    this.name = "SealError";
  }
}
function sealBytes(plaintext, key, opts) {
  if (key.length !== 32) {
    throw new Error("AEAD key must be 32 bytes — call normalizeKey() first");
  }
  const version = opts.version ?? 1;
  if (version < 1 || version > 255) {
    throw new Error(`AEAD envelope version must fit in one byte; got ${version}`);
  }
  const nonce = randomBytes(NONCE_LEN);
  const ciphertext = xchacha20poly1305(key, nonce, opts.aad).encrypt(plaintext);
  const wire = new Uint8Array(VERSION_LEN + NONCE_LEN + ciphertext.length);
  wire[0] = version;
  wire.set(nonce, VERSION_LEN);
  wire.set(ciphertext, VERSION_LEN + NONCE_LEN);
  return wire;
}
function openBytes(envelope, key, opts) {
  if (key.length !== 32) {
    throw new Error("AEAD key must be 32 bytes — call normalizeKey() first");
  }
  if (envelope.length < MIN_ENVELOPE_LEN) {
    throw new SealError("envelope truncated");
  }
  const expectedVersion = opts.version ?? 1;
  if (envelope[0] !== expectedVersion) {
    throw new SealError(`unsupported envelope version: ${envelope[0]}`);
  }
  const nonce = envelope.subarray(VERSION_LEN, VERSION_LEN + NONCE_LEN);
  const ciphertext = envelope.subarray(VERSION_LEN + NONCE_LEN);
  try {
    return xchacha20poly1305(key, nonce, opts.aad).decrypt(ciphertext);
  } catch {
    throw new SealError("envelope verification failed");
  }
}

// src/http/token.ts
var _UTF8 = new TextEncoder;
var TOKEN_VERSION = 5;
var CALL_TOKEN_VERSION = 1;
var CALL_ID_LEN = 16;
var AAD_PREFIX = _UTF8.encode("vgi_rpc.state.v4\x00");
var CALL_AAD_PREFIX = _UTF8.encode("vgi_rpc.call.v1\x00");
function computeAad(principal) {
  return aadWith(AAD_PREFIX, principal);
}
function computeCallAad(principal) {
  return aadWith(CALL_AAD_PREFIX, principal);
}
function aadWith(prefix, principal) {
  if (!principal) {
    const tail2 = _UTF8.encode("\x00anonymous");
    return concatBytes2(prefix, tail2);
  }
  const pBytes = _UTF8.encode(principal);
  const tail = new Uint8Array(1 + pBytes.length);
  tail[0] = 1;
  tail.set(pBytes, 1);
  return concatBytes2(prefix, tail);
}
function bytesToBase64(bytes) {
  let s = "";
  for (let i = 0;i < bytes.length; i += 32768) {
    s += String.fromCharCode(...bytes.subarray(i, i + 32768));
  }
  return btoa(s);
}
function base64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0;i < bin.length; i++)
    out[i] = bin.charCodeAt(i);
  return out;
}
function writeU32LE(view, offset, value) {
  view.setUint32(offset, value, true);
}
function writeU64LE(view, offset, value) {
  view.setBigUint64(offset, value, true);
}
function readU32LE(view, offset) {
  return view.getUint32(offset, true);
}
function readU64LE(view, offset) {
  return view.getBigUint64(offset, true);
}
function concatBytes2(...parts) {
  let total = 0;
  for (const p of parts)
    total += p.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}
function packStateToken(stateBytes, callId, tokenKey, principal, createdAt) {
  if (tokenKey.length !== 32) {
    throw new Error("XChaCha20-Poly1305 token key must be 32 bytes");
  }
  const now = createdAt ?? Math.floor(Date.now() / 1000);
  const plaintext = new Uint8Array(8 + CALL_ID_LEN + 4 + stateBytes.length);
  const view = new DataView(plaintext.buffer);
  let offset = 0;
  writeU64LE(view, offset, BigInt(now));
  offset += 8;
  plaintext.set(callId, offset);
  offset += CALL_ID_LEN;
  writeU32LE(view, offset, stateBytes.length);
  offset += 4;
  plaintext.set(stateBytes, offset);
  const wire = sealBytes(plaintext, tokenKey, { aad: computeAad(principal), version: TOKEN_VERSION });
  return bytesToBase64(wire);
}
function packCallToken(callId, schemaBytes, inputSchemaBytes, tokenKey, principal, createdAt) {
  if (tokenKey.length !== 32) {
    throw new Error("XChaCha20-Poly1305 token key must be 32 bytes");
  }
  const now = createdAt ?? Math.floor(Date.now() / 1000);
  const plaintext = new Uint8Array(8 + CALL_ID_LEN + 4 + schemaBytes.length + 4 + inputSchemaBytes.length);
  const view = new DataView(plaintext.buffer);
  let offset = 0;
  writeU64LE(view, offset, BigInt(now));
  offset += 8;
  plaintext.set(callId, offset);
  offset += CALL_ID_LEN;
  writeU32LE(view, offset, schemaBytes.length);
  offset += 4;
  plaintext.set(schemaBytes, offset);
  offset += schemaBytes.length;
  writeU32LE(view, offset, inputSchemaBytes.length);
  offset += 4;
  plaintext.set(inputSchemaBytes, offset);
  const wire = sealBytes(plaintext, tokenKey, {
    aad: computeCallAad(principal),
    version: CALL_TOKEN_VERSION
  });
  return bytesToBase64(wire);
}
function unpackStateToken(tokenBase64, tokenKey, tokenTtl, principal) {
  let raw;
  try {
    raw = base64ToBytes(tokenBase64);
  } catch {
    throw new Error("Malformed state token");
  }
  if (raw.length >= 1 && raw[0] !== TOKEN_VERSION) {
    throw new Error(`Unsupported state token version: ${raw[0]}`);
  }
  let plaintext;
  try {
    plaintext = openBytes(raw, tokenKey, { aad: computeAad(principal), version: TOKEN_VERSION });
  } catch (err2) {
    if (err2 instanceof SealError) {
      throw new Error("State token signature verification failed");
    }
    throw err2;
  }
  if (plaintext.length < 8 + CALL_ID_LEN) {
    throw new Error("State token truncated");
  }
  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  let offset = 0;
  const copyAligned = (start, len) => {
    const out = new Uint8Array(len);
    out.set(plaintext.subarray(start, start + len));
    return out;
  };
  const createdAt = Number(readU64LE(view, offset));
  offset += 8;
  if (tokenTtl > 0) {
    const now = Math.floor(Date.now() / 1000);
    if (now - createdAt > tokenTtl) {
      throw new Error("State token expired");
    }
  }
  const callId = copyAligned(offset, CALL_ID_LEN);
  offset += CALL_ID_LEN;
  const stateLen = readU32LE(view, offset);
  offset += 4;
  if (offset + stateLen > plaintext.length) {
    throw new Error("State token truncated (state)");
  }
  const stateBytes = copyAligned(offset, stateLen);
  return { stateBytes, callId, createdAt };
}
function unpackCallToken(token, tokenKey, principal, tokenTtl = 0) {
  const raw = base64ToBytes(token);
  if (raw.length >= 1 && raw[0] !== CALL_TOKEN_VERSION) {
    throw new Error(`Unsupported call token version ${raw[0]}`);
  }
  let plaintext;
  try {
    plaintext = openBytes(raw, tokenKey, {
      aad: computeCallAad(principal),
      version: CALL_TOKEN_VERSION
    });
  } catch (err2) {
    if (err2 instanceof Error && /decrypt|auth|tag/i.test(err2.message)) {
      throw new Error("State token signature verification failed");
    }
    throw err2;
  }
  if (plaintext.length < 8 + CALL_ID_LEN) {
    throw new Error("State token truncated");
  }
  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  const copyAligned = (start, len) => {
    const out = new Uint8Array(len);
    out.set(plaintext.subarray(start, start + len));
    return out;
  };
  let offset = 0;
  const createdAt = Number(readU64LE(view, offset));
  offset += 8;
  if (tokenTtl > 0) {
    const now = Math.floor(Date.now() / 1000);
    if (now - createdAt > tokenTtl) {
      throw new Error("State token expired");
    }
  }
  const callId = copyAligned(offset, CALL_ID_LEN);
  offset += CALL_ID_LEN;
  const schemaLen = readU32LE(view, offset);
  offset += 4;
  if (offset + schemaLen > plaintext.length) {
    throw new Error("State token truncated (schema)");
  }
  const schemaBytes = copyAligned(offset, schemaLen);
  offset += schemaLen;
  const inputSchemaLen = readU32LE(view, offset);
  offset += 4;
  if (offset + inputSchemaLen > plaintext.length) {
    throw new Error("State token truncated (input schema)");
  }
  const inputSchemaBytes = copyAligned(offset, inputSchemaLen);
  return { callId, call: { schemaBytes, inputSchemaBytes } };
}

// src/http/dispatch.ts
function dispatchDebug() {
  const env = globalThis.process?.env;
  return Boolean(env?.VGI_DISPATCH_DEBUG);
}
var CALL_STATE_CACHE_ENTRIES = 4096;
var callStates = new Map;
function callCacheKey(callId, principal) {
  let hex = "";
  for (const b of callId)
    hex += b.toString(16).padStart(2, "0");
  return `${hex}\x00${principal ?? ""}`;
}
function cacheEntriesFor(ctx) {
  return ctx.callStateCacheEntries ?? CALL_STATE_CACHE_ENTRIES;
}
function cacheCall(callId, ctx, call) {
  const limit = cacheEntriesFor(ctx);
  if (limit <= 0)
    return;
  if (callStates.size >= limit) {
    callStates.clear();
  }
  const ttl = ctx.tokenTtl;
  callStates.set(callCacheKey(callId, ctx.authContext?.principal), {
    expiresAt: Math.floor(Date.now() / 1000) + (ttl > 0 ? ttl : 3600),
    call
  });
}
function newCallId() {
  const id = new Uint8Array(CALL_ID_LEN);
  crypto.getRandomValues(id);
  return id;
}
function resolveCall(callId, callTokenB64, ctx) {
  const principal = ctx.authContext?.principal;
  const key = callCacheKey(callId, principal);
  const hit = cacheEntriesFor(ctx) > 0 ? callStates.get(key) : undefined;
  if (hit) {
    if (Math.floor(Date.now() / 1000) <= hit.expiresAt)
      return hit.call;
    callStates.delete(key);
  }
  if (!callTokenB64) {
    throw new HttpRpcError("Missing call token in exchange request", 400);
  }
  const { callId: tokenCallId, call } = unpackCallToken(callTokenB64, ctx.tokenKey, principal, ctx.tokenTtl);
  if (tokenCallId.length !== callId.length || !tokenCallId.every((b, i) => b === callId[i])) {
    throw new HttpRpcError("Invalid state token: Malformed state token", 400);
  }
  cacheCall(callId, ctx, call);
  return call;
}
function mintInitTokens(stateBytes, schemaBytes, inputSchemaBytes, ctx) {
  const callId = newCallId();
  noteStream(ctx, callId);
  const principal = ctx.authContext?.principal;
  const callToken = packCallToken(callId, schemaBytes, inputSchemaBytes, ctx.tokenKey, principal);
  cacheCall(callId, ctx, { schemaBytes, inputSchemaBytes });
  return {
    callId,
    token: packStateToken(stateBytes, callId, ctx.tokenKey, principal),
    callToken
  };
}
function noteStream(ctx, callId) {
  const observer = ctx.streamObserver;
  if (!observer)
    return;
  let hex = "";
  for (const b of callId)
    hex += b.toString(16).padStart(2, "0");
  observer.streamId = hex;
}
async function deserializeSchema3(bytes) {
  return deserializeSchema(bytes);
}
var EMPTY_SCHEMA = schema([]);
function countExternalized(ctx) {
  const egress = ctx.egress;
  if (!egress)
    return;
  return (bytes) => {
    egress.externalizedBytes += bytes;
  };
}
function predictExternalizeBytes(batch, config) {
  if (!config?.storage)
    return 0;
  if (batch.numRows === 0)
    return 0;
  const size = batch.data?.byteLength ?? 0;
  const threshold = config.externalizeThresholdBytes ?? 1048576;
  if (size < threshold)
    return 0;
  return size;
}
function makeCapErrorResponse(schema2, error, ctx) {
  const errBatch = buildErrorBatch(schema2, error, ctx.serverId, null);
  const response = arrowResponse(serializeIpcStream(schema2, [errBatch]), 500);
  response.__dispatchError = error;
  return response;
}
async function httpDispatchDescribe(protocolName, methods, serverId, protocolVersion) {
  const { batch } = await buildDescribeBatch(protocolName, methods, serverId, protocolVersion);
  const body = serializeIpcStream(DESCRIBE_SCHEMA, [batch]);
  return arrowResponse(body);
}
async function httpDispatchUnary(method, body, ctx) {
  const schema2 = method.resultSchema;
  const { schema: reqSchema, batch: reqBatchRaw } = await readRequestFromBody(body);
  let reqBatch = reqBatchRaw;
  let effectiveSchema = reqSchema;
  if (ctx.externalLocation && isExternalLocationBatch(reqBatchRaw)) {
    const resolved = await resolveExternalLocation(reqBatchRaw, ctx.externalLocation);
    const mergedMeta = new Map(resolved.metadata ?? []);
    for (const [k, v] of reqBatchRaw.metadata ?? []) {
      mergedMeta.set(k, v);
    }
    reqBatch = withBatchMetadata(resolved, mergedMeta);
    effectiveSchema = resolved.schema;
  }
  const parsed = parseRequest(effectiveSchema, reqBatch);
  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(`Method name in request '${parsed.methodName}' does not match URL '${method.name}'`, 400);
  }
  applyDefaults(parsed.params, method.defaults);
  const externalizationEnabled = !!ctx.externalLocation?.storage;
  const out = new OutputCollector(schema2, true, ctx.serverId, parsed.requestId, ctx.authContext, ctx.cookies, ctx.kind ?? "http" /* HTTP */, {
    remainingResponseBytes: ctx.maxResponseBytes,
    remainingExternalizedResponseBytes: externalizationEnabled ? ctx.maxExternalizedResponseBytes : undefined,
    externalizationEnabled
  });
  out.enableCookieSink();
  if (ctx.stickyContext)
    out.attachStickyContext(ctx.stickyContext);
  try {
    const result = await method.handler(parsed.params, out);
    let resultBatch = buildResultBatch(schema2, result, ctx.serverId, parsed.requestId);
    if (ctx.externalLocation) {
      const predicted = predictExternalizeBytes(resultBatch, ctx.externalLocation);
      if (ctx.maxExternalizedResponseBytes != null && predicted > ctx.maxExternalizedResponseBytes) {
        const overshoot = new Error(`Externalised payload exceeds max_externalized_response_bytes (${predicted} > ${ctx.maxExternalizedResponseBytes}) for method '${method.name}'`);
        overshoot.name = "RuntimeError";
        const response2 = makeCapErrorResponse(schema2, overshoot, ctx);
        appendCookieHeaders(response2.headers, out.drainResponseCookies());
        return response2;
      }
      resultBatch = await maybeExternalizeBatch(resultBatch, ctx.externalLocation, countExternalized(ctx));
    }
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    const body2 = serializeIpcStream(schema2, batches);
    if (ctx.maxResponseBytes != null && body2.byteLength > ctx.maxResponseBytes) {
      const overshoot = new Error(`HTTP body exceeds max_response_bytes (${body2.byteLength} > ${ctx.maxResponseBytes}) for method '${method.name}'`);
      overshoot.name = "RuntimeError";
      const response2 = makeCapErrorResponse(schema2, overshoot, ctx);
      appendCookieHeaders(response2.headers, out.drainResponseCookies());
      return response2;
    }
    const response = arrowResponse(body2);
    appendCookieHeaders(response.headers, out.drainResponseCookies());
    return response;
  } catch (error) {
    const errBatch = buildErrorBatch(schema2, error, ctx.serverId, parsed.requestId);
    const response = arrowResponse(serializeIpcStream(schema2, [errBatch]), 500);
    appendCookieHeaders(response.headers, out.drainResponseCookies());
    response.__dispatchError = error;
    return response;
  }
}
async function httpDispatchStreamInit(method, body, ctx) {
  const isProducer = !!method.producerFn;
  const outputSchema = method.outputSchema;
  const inputSchema = method.inputSchema ?? EMPTY_SCHEMA;
  const { schema: reqSchema, batch: reqBatch } = await readRequestFromBody(body);
  const parsed = parseRequest(reqSchema, reqBatch);
  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(`Method name in request '${parsed.methodName}' does not match URL '${method.name}'`, 400);
  }
  applyDefaults(parsed.params, method.defaults);
  let state;
  try {
    if (isProducer) {
      state = await method.producerInit(parsed.params);
    } else {
      state = await method.exchangeInit(parsed.params);
    }
  } catch (error) {
    const errSchema = method.headerSchema ?? EMPTY_SCHEMA;
    const errBatch = buildErrorBatch(errSchema, error, ctx.serverId, parsed.requestId);
    const response = arrowResponse(serializeIpcStream(errSchema, [errBatch]), 500);
    response.__dispatchError = error;
    return response;
  }
  const resolvedOutputSchema = state?.__outputSchema ?? outputSchema;
  const resolvedInputSchema = state?.__inputSchema ?? inputSchema;
  const effectiveProducer = state?.__isProducer ?? isProducer;
  let headerBytes = null;
  if (method.headerSchema && method.headerInit) {
    try {
      const headerOut = new OutputCollector(method.headerSchema, true, ctx.serverId, parsed.requestId, ctx.authContext, ctx.cookies, ctx.kind ?? "http" /* HTTP */);
      const headerValues = method.headerInit(parsed.params, state, headerOut);
      const headerBatch = buildResultBatch(method.headerSchema, headerValues, ctx.serverId, parsed.requestId);
      const headerBatches = [...headerOut.batches.map((b) => b.batch), headerBatch];
      headerBytes = serializeIpcStream(method.headerSchema, headerBatches);
    } catch (error) {
      const errBatch = buildErrorBatch(method.headerSchema, error, ctx.serverId, parsed.requestId);
      const response = arrowResponse(serializeIpcStream(method.headerSchema, [errBatch]), 500);
      response.__dispatchError = error;
      return response;
    }
  }
  if (effectiveProducer) {
    const initCallId = newCallId();
    noteStream(ctx, initCallId);
    const initCallToken = packCallToken(initCallId, serializeSchema2(resolvedOutputSchema), serializeSchema2(resolvedInputSchema), ctx.tokenKey, ctx.authContext?.principal);
    cacheCall(initCallId, ctx, {
      schemaBytes: serializeSchema2(resolvedOutputSchema),
      inputSchemaBytes: serializeSchema2(resolvedInputSchema)
    });
    return produceStreamResponse(method, state, resolvedOutputSchema, resolvedInputSchema, ctx, parsed.requestId, headerBytes, { callId: initCallId, callToken: initCallToken }, reqBatch.metadata ?? undefined);
  } else {
    const stateBytes = ctx.stateSerializer.serialize(state);
    const schemaBytes = serializeSchema2(resolvedOutputSchema);
    const inputSchemaBytes = serializeSchema2(resolvedInputSchema);
    const { token, callToken } = mintInitTokens(stateBytes, schemaBytes, inputSchemaBytes, ctx);
    const tokenMeta = new Map;
    tokenMeta.set(STATE_KEY, token);
    tokenMeta.set(CALL_STATE_KEY, callToken);
    const tokenBatch = buildEmptyBatch(resolvedOutputSchema, tokenMeta);
    const tokenStreamBytes = serializeIpcStream(resolvedOutputSchema, [tokenBatch]);
    let responseBody;
    if (headerBytes) {
      responseBody = concatBytes3(headerBytes, tokenStreamBytes);
    } else {
      responseBody = tokenStreamBytes;
    }
    return arrowResponse(responseBody);
  }
}
async function httpDispatchStreamExchange(method, body, ctx) {
  const isProducer = !!method.producerFn;
  const { batch: reqBatch } = await readRequestFromBody(body);
  const tokenBase64 = reqBatch.metadata?.get(STATE_KEY);
  if (!tokenBase64) {
    throw new HttpRpcError("Missing state token in exchange request", 400);
  }
  const cancelled = reqBatch.metadata?.get(CANCEL_KEY) != null;
  if (cancelled && ctx.streamObserver)
    ctx.streamObserver.cancelled = true;
  let unpacked;
  try {
    unpacked = unpackStateToken(tokenBase64, ctx.tokenKey, ctx.tokenTtl, ctx.authContext?.principal);
  } catch (error) {
    throw new HttpRpcError(`Invalid state token: ${error.message}`, 400);
  }
  noteStream(ctx, unpacked.callId);
  const resolvedCall = resolveCall(unpacked.callId, reqBatch.metadata?.get(CALL_STATE_KEY), ctx);
  let state;
  try {
    state = ctx.stateSerializer.deserialize(unpacked.stateBytes);
  } catch (error) {
    console.error(`[httpDispatchStreamExchange] state deserialize error:`, error.message);
    throw new HttpRpcError(`State deserialization failed: ${error.message}`, 500);
  }
  let outputSchema;
  if (resolvedCall.schemaBytes.length > 0) {
    outputSchema = await deserializeSchema3(resolvedCall.schemaBytes);
  } else {
    outputSchema = state?.__outputSchema ?? method.outputSchema;
  }
  let inputSchema;
  if (resolvedCall.inputSchemaBytes.length > 0) {
    inputSchema = await deserializeSchema3(resolvedCall.inputSchemaBytes);
  } else {
    inputSchema = state?.__inputSchema ?? method.inputSchema ?? EMPTY_SCHEMA;
  }
  const effectiveProducer = state?.__isProducer ?? isProducer;
  if (dispatchDebug())
    console.error(`[httpDispatchStreamExchange] method=${method.name} effectiveProducer=${effectiveProducer} stateKeys=${Object.keys(state || {})}`);
  if (cancelled) {
    if (method.onCancel) {
      try {
        await method.onCancel(state);
      } catch (err2) {
        console.debug?.(`onCancel hook failed: ${err2 instanceof Error ? err2.message : err2}`);
      }
    }
    return arrowResponse(serializeIpcStream(outputSchema, []));
  }
  if (effectiveProducer) {
    return produceStreamResponse(method, state, outputSchema, inputSchema, ctx, null, null, { callId: unpacked.callId, callToken: null }, reqBatch.metadata ?? undefined);
  } else {
    const externalizationEnabled = !!ctx.externalLocation?.storage;
    const out = new OutputCollector(outputSchema, effectiveProducer, ctx.serverId, null, ctx.authContext, ctx.cookies, ctx.kind ?? "http" /* HTTP */, {
      remainingResponseBytes: ctx.maxResponseBytes,
      remainingExternalizedResponseBytes: externalizationEnabled ? ctx.maxExternalizedResponseBytes : undefined,
      externalizationEnabled
    });
    if (ctx.stickyContext)
      out.attachStickyContext(ctx.stickyContext);
    let conformedBatch = reqBatch;
    if (!effectiveProducer && inputSchema !== EMPTY_SCHEMA && reqBatch.schema !== inputSchema) {
      try {
        conformedBatch = conformBatchToSchema(reqBatch, inputSchema);
      } catch (e) {
        if (e instanceof TypeError)
          throw e;
        console.debug?.(`Schema conformance skipped: ${e instanceof Error ? e.message : e}`);
      }
    }
    try {
      if (method.exchangeFn) {
        await method.exchangeFn(state, conformedBatch, out);
      } else {
        await method.producerFn(state, out);
      }
    } catch (error) {
      if (dispatchDebug())
        console.error(`[httpDispatchStreamExchange] exchange handler error:`, error.message, error.stack?.split(`
`).slice(0, 5).join(`
`));
      const errBatch = buildErrorBatch(outputSchema, error, ctx.serverId, null);
      const response = arrowResponse(serializeIpcStream(outputSchema, [errBatch]), 500);
      response.__dispatchError = error;
      return response;
    }
    const batches = [];
    if (out.finished) {
      for (const emitted of out.batches) {
        if (emitted.metadata && emitted.metadata.size > 0) {
          const md = new Map(emitted.batch.metadata ?? []);
          for (const [k, v] of emitted.metadata)
            md.set(k, v);
          batches.push(withBatchMetadata(emitted.batch, md));
        } else {
          batches.push(emitted.batch);
        }
      }
    } else {
      const stateBytes = ctx.stateSerializer.serialize(state);
      const token = packStateToken(stateBytes, unpacked.callId, ctx.tokenKey, ctx.authContext?.principal);
      for (const [idx, emitted] of out.batches.entries()) {
        const batch = emitted.batch;
        if (idx === out.dataBatchIdx) {
          const mergedMeta = new Map(batch.metadata ?? []);
          if (emitted.metadata)
            for (const [k, v] of emitted.metadata)
              mergedMeta.set(k, v);
          mergedMeta.set(STATE_KEY, token);
          batches.push(withBatchMetadata(batch, mergedMeta));
        } else {
          batches.push(batch);
        }
      }
      if (!batches.some((b) => b.metadata?.get(STATE_KEY))) {
        const tokenMeta = new Map;
        tokenMeta.set(STATE_KEY, token);
        batches.push(buildEmptyBatch(outputSchema, tokenMeta));
      }
    }
    const body2 = serializeIpcStream(outputSchema, batches);
    if (ctx.maxResponseBytes != null && body2.byteLength > ctx.maxResponseBytes) {
      const overshoot = new Error(`HTTP body exceeds max_response_bytes (${body2.byteLength} > ${ctx.maxResponseBytes}) for method '${method.name}'`);
      overshoot.name = "RuntimeError";
      return makeCapErrorResponse(outputSchema, overshoot, ctx);
    }
    return arrowResponse(body2);
  }
}
async function produceStreamResponse(method, state, outputSchema, inputSchema, ctx, requestId, headerBytes, call, requestMetadata) {
  const allBatches = [];
  const maxBytes = ctx.maxStreamResponseBytes ?? ctx.maxResponseBytes;
  const maxExternalBytes = ctx.maxExternalizedResponseBytes;
  const externalizationEnabled = !!ctx.externalLocation?.storage;
  let estimatedBytes = 0;
  let cumulativeExternalBytes = 0;
  let producerError;
  let externalOvershoot;
  let firstTick = true;
  while (true) {
    const remainingWire = maxBytes != null ? Math.max(0, maxBytes - estimatedBytes) : undefined;
    const remainingExternal = externalizationEnabled && maxExternalBytes != null ? Math.max(0, maxExternalBytes - cumulativeExternalBytes) : undefined;
    const out = new OutputCollector(outputSchema, true, ctx.serverId, requestId, ctx.authContext, ctx.cookies, ctx.kind ?? "http" /* HTTP */, {
      remainingResponseBytes: remainingWire,
      remainingExternalizedResponseBytes: remainingExternal,
      externalizationEnabled
    });
    if (ctx.stickyContext)
      out.attachStickyContext(ctx.stickyContext);
    try {
      if (method.producerFn) {
        await method.producerFn(state, out);
      } else {
        const tickBatch = buildEmptyBatch(inputSchema, firstTick ? requestMetadata : undefined);
        await method.exchangeFn(state, tickBatch, out);
      }
      firstTick = false;
    } catch (error) {
      if (dispatchDebug())
        console.error(`[produceStreamResponse] error:`, error.message, error.stack?.split(`
`).slice(0, 3).join(`
`));
      allBatches.push(buildErrorBatch(outputSchema, error, ctx.serverId, requestId));
      producerError = error instanceof Error ? error : new Error(String(error));
      break;
    }
    for (const emitted of out.batches) {
      let batch = emitted.batch;
      if (externalizationEnabled && ctx.externalLocation) {
        const predicted = predictExternalizeBytes(batch, ctx.externalLocation);
        if (predicted > 0 && maxExternalBytes != null && cumulativeExternalBytes + predicted > maxExternalBytes) {
          externalOvershoot = new Error(`Externalised payload exceeds max_externalized_response_bytes (${cumulativeExternalBytes + predicted} > ${maxExternalBytes}) for method '${method.name}'`);
          externalOvershoot.name = "RuntimeError";
          break;
        }
        if (predicted > 0) {
          batch = await maybeExternalizeBatch(batch, ctx.externalLocation, countExternalized(ctx));
          cumulativeExternalBytes += predicted;
        }
      }
      if (emitted.metadata && emitted.metadata.size > 0) {
        const md = new Map(batch.metadata ?? []);
        for (const [k, v] of emitted.metadata)
          md.set(k, v);
        batch = withBatchMetadata(batch, md);
      }
      allBatches.push(batch);
      if (maxBytes != null) {
        let sz = batch.data?.byteLength ?? 0;
        if (sz === 0) {
          try {
            sz = serializeBatch(batch).byteLength;
          } catch {
            sz = 0;
          }
          if (sz === 0)
            sz = 1;
        }
        estimatedBytes += sz;
      }
    }
    if (externalOvershoot) {
      allBatches.length = 0;
      allBatches.push(buildErrorBatch(outputSchema, externalOvershoot, ctx.serverId, requestId));
      producerError = externalOvershoot;
      break;
    }
    if (out.finished) {
      break;
    }
    if (maxBytes != null && estimatedBytes >= maxBytes) {
      const stateBytes = ctx.stateSerializer.serialize(state);
      const token = packStateToken(stateBytes, call.callId, ctx.tokenKey, ctx.authContext?.principal);
      const tokenMeta = new Map;
      tokenMeta.set(STATE_KEY, token);
      if (call.callToken)
        tokenMeta.set(CALL_STATE_KEY, call.callToken);
      allBatches.push(buildEmptyBatch(outputSchema, tokenMeta));
      break;
    }
  }
  const dataBytes = serializeIpcStream(outputSchema, allBatches);
  let responseBody;
  if (headerBytes) {
    responseBody = concatBytes3(headerBytes, dataBytes);
  } else {
    responseBody = dataBytes;
  }
  const status = externalOvershoot ? 500 : 200;
  const response = arrowResponse(responseBody, status);
  if (producerError) {
    response.__dispatchError = producerError;
  }
  return response;
}
function concatBytes3(...arrays) {
  const totalLen = arrays.reduce((sum, a) => sum + a.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

// src/http/introspect.ts
var INTROSPECT_ENDPOINT = "/__introspect_token__";
var INTROSPECT_ENABLED_HEADER = "VGI-Token-Introspection";
var JWS_SHAPED = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/;
var MAX_BODY_BYTES = 8192;
var MAX_TOKEN_CHARS = 4096;
var DEFAULT_INTROSPECT_TTL_SECONDS = 300;
async function tokenDigest(token) {
  return sha256Hex2(new TextEncoder().encode(token));
}

class RateLimiter {
  perWindow;
  windowMs;
  counts = new Map;
  windowStart = 0;
  constructor(perWindow, windowMs = 1000) {
    this.perWindow = perWindow;
    this.windowMs = windowMs;
  }
  allow(key, now = Date.now()) {
    if (now - this.windowStart >= this.windowMs) {
      this.counts.clear();
      this.windowStart = now;
    }
    const count = this.counts.get(key) ?? 0;
    if (count >= this.perWindow)
      return false;
    this.counts.set(key, count + 1);
    return true;
  }
}
function createIntrospector(options) {
  const principals = new Set([...options.principals ?? []].filter((p) => p));
  if (principals.size === 0) {
    throw new Error("introspectPrincipals must name at least one principal. Introspection is a " + "distinct capability from authentication: allowing any authenticated caller " + "lets any user resolve any other user's credential to its owner.");
  }
  const ttlSeconds = options.ttlSeconds ?? DEFAULT_INTROSPECT_TTL_SECONDS;
  if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) {
    throw new Error("introspectTtlSeconds must be a finite, positive number of seconds");
  }
  const limiter = new RateLimiter(options.rateLimit ?? 20);
  return {
    handle: (request, auth) => introspect(request, auth, options.resolver, principals, ttlSeconds, limiter)
  };
}
function refuse(status, error, extra) {
  const headers = new Headers({
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    ...extra
  });
  return new Response(JSON.stringify({ error }), { status, headers });
}
function introspectionDisabledResponse() {
  return refuse(404, "not_enabled");
}
async function readSubjectToken(request) {
  const declared = request.headers.get("Content-Length");
  if (declared && Number(declared) > MAX_BODY_BYTES)
    return null;
  const raw = new Uint8Array(await request.arrayBuffer());
  if (raw.byteLength > MAX_BODY_BYTES)
    return null;
  let body;
  try {
    body = JSON.parse(new TextDecoder().decode(raw));
  } catch {
    return null;
  }
  if (typeof body !== "object" || body === null || Array.isArray(body))
    return null;
  const token = body.token;
  if (typeof token !== "string" || !token || token.length > MAX_TOKEN_CHARS)
    return null;
  return token;
}
async function introspect(request, auth, resolver, principals, defaultTtlSeconds, limiter) {
  const caller = auth?.principal ?? "";
  if (!auth?.authenticated || !principals.has(caller)) {
    console.warn("[introspect] refused: caller is not an introspector", { principal: caller });
    return refuse(403, "not_an_introspector");
  }
  if (!limiter.allow(caller)) {
    console.warn("[introspect] rate limit exceeded", { principal: caller });
    return refuse(429, "rate_limited", { "Retry-After": "1" });
  }
  const token = await readSubjectToken(request);
  if (token === null) {
    return refuse(404, "unresolved");
  }
  const digest = await tokenDigest(token);
  if (JWS_SHAPED.test(token)) {
    console.warn("[introspect] refused: JWS-shaped subject", { principal: caller, tokenDigest: digest });
    return refuse(404, "unresolved");
  }
  const identity = await resolver(token);
  if (identity == null) {
    console.info("[introspect] credential did not resolve", { principal: caller, tokenDigest: digest });
    return refuse(404, "unresolved");
  }
  console.info("[introspect] resolved", {
    principal: caller,
    tokenDigest: digest,
    resolvedPrincipal: identity.principal
  });
  const body = JSON.stringify({
    principal: identity.principal,
    token_name: identity.tokenName ?? "",
    ttl_seconds: identity.ttlSeconds ?? defaultTtlSeconds
  });
  return new Response(body, {
    status: 200,
    headers: new Headers({ "Content-Type": "application/json", "Cache-Control": "no-store" })
  });
}

// src/http/pages.ts
var LOGO_URL = "https://vgi-rpc-python.query.farm/assets/logo-hero.png";
var FONTS = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">`;
var ERROR_PAGE_STYLE = `<style>
body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
       margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
       background: #faf8f0; }
.logo { margin-bottom: 24px; }
.logo img { width: 120px; height: 120px; border-radius: 50%;
             box-shadow: 0 4px 24px rgba(0,0,0,0.12); }
h1 { color: #2d5016; margin-bottom: 8px; font-weight: 700; }
code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
        padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }
a { color: #2d5016; text-decoration: none; }
a:hover { color: #4a7c23; }
p { line-height: 1.7; color: #6b6b5a; }
.detail { margin-top: 12px; padding: 12px 16px; background: #f0ece0;
           border-radius: 6px; font-size: 0.9em; color: #6b6b5a; }
footer { margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
          color: #6b6b5a; font-size: 0.85em; line-height: 1.8; }
footer a { color: #2d5016; font-weight: 600; }
footer a:hover { color: #4a7c23; }
</style>`;
function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function arrowTypeToString(type) {
  const id = type.typeId;
  if (id === 5)
    return "str";
  if (id === 4)
    return "bytes";
  if (id === 2)
    return "int";
  if (id === 3)
    return "float";
  if (id === 6)
    return "bool";
  if (id === 12)
    return "list";
  if (id === 17)
    return "map";
  if (id === 24)
    return "enum";
  return type.toString();
}
function buildLandingPage(protocolName, serverId, describePath, repoUrl) {
  const links = [];
  if (describePath) {
    links.push(`<a class="primary" href="${escapeHtml(describePath)}">View service API</a>`);
  }
  if (repoUrl) {
    links.push(`<a href="${escapeHtml(repoUrl)}">Source repository</a>`);
  }
  links.push(`<a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>`);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(protocolName)} — vgi-rpc</title>
${FONTS}
<style>
body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
       margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
       background: #faf8f0; }
.logo { margin-bottom: 24px; }
.logo img { width: 140px; height: 140px; border-radius: 50%;
             box-shadow: 0 4px 24px rgba(0,0,0,0.12); }
h1 { color: #2d5016; margin-bottom: 8px; font-weight: 700; }
code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
        padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }
a { color: #2d5016; text-decoration: none; }
a:hover { color: #4a7c23; }
p { line-height: 1.7; color: #6b6b5a; }
.meta { font-size: 0.9em; color: #6b6b5a; }
.links { margin-top: 28px; display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; }
.links a { display: inline-block; padding: 8px 18px; border-radius: 6px;
            border: 1px solid #4a7c23; color: #2d5016; font-weight: 600;
            font-size: 0.9em; transition: all 0.2s ease; }
.links a:hover { background: #4a7c23; color: #fff; }
.links a.primary { background: #2d5016; color: #fff; border-color: #2d5016; }
.links a.primary:hover { background: #4a7c23; border-color: #4a7c23; }
footer { margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
          color: #6b6b5a; font-size: 0.85em; }
footer a { color: #2d5016; font-weight: 600; }
footer a:hover { color: #4a7c23; }
</style>
</head>
<body>
<div class="logo">
  <img src="${LOGO_URL}" alt="vgi-rpc logo">
</div>
<h1>${escapeHtml(protocolName)}</h1>
<p class="meta">Powered by <code>vgi-rpc</code> (TypeScript) &middot; server <code>${escapeHtml(serverId)}</code></p>
<p>This is a <code>vgi-rpc</code> service endpoint.</p>
<div class="links">
${links.join(`
`)}
</div>
<footer>
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>`;
}
function buildNotFoundPage(prefix, protocolName) {
  const nameFragment = protocolName ? ` (<strong>${escapeHtml(protocolName)}</strong>)` : "";
  const prefixDisplay = prefix || "/";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>404 — vgi-rpc</title>
${FONTS}
${ERROR_PAGE_STYLE}
</head>
<body>
<div class="logo">
  <img src="${LOGO_URL}" alt="vgi-rpc logo">
</div>
<h1>404 — Not Found</h1>
<p>This is a <code>vgi-rpc</code> service endpoint${nameFragment}.</p>
<p>RPC methods are available under <code>${escapeHtml(prefixDisplay)}/&lt;method&gt;</code>.</p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>`;
}
function buildMethodCard(method) {
  const name = escapeHtml(method.name);
  const isUnary = method.type === "unary";
  const hasHeader = !!method.headerSchema;
  const badges = [];
  badges.push(isUnary ? `<span class="badge badge-unary">unary</span>` : `<span class="badge badge-stream">stream</span>`);
  if (hasHeader)
    badges.push(`<span class="badge badge-header">header</span>`);
  let paramsHtml = "";
  const paramsSchema = method.paramsSchema;
  if (paramsSchema.fields.length > 0) {
    const rows = paramsSchema.fields.map((f) => {
      const paramName = escapeHtml(f.name);
      const paramType = escapeHtml(arrowTypeToString(f.type));
      const defaultVal = method.defaults && f.name in method.defaults ? escapeHtml(JSON.stringify(method.defaults[f.name])) : "&mdash;";
      return `<tr><td><code>${paramName}</code></td><td><code>${paramType}</code></td><td>${defaultVal}</td><td>&mdash;</td></tr>`;
    });
    paramsHtml = `<div class="section-label">Parameters</div>
<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>
${rows.join(`
`)}
</table>`;
  } else {
    paramsHtml = `<p class="no-params">No parameters</p>`;
  }
  let returnsHtml = "";
  if (isUnary && method.resultSchema.fields.length > 0) {
    const rows = method.resultSchema.fields.map((f) => {
      return `<tr><td><code>${escapeHtml(f.name)}</code></td><td><code>${escapeHtml(arrowTypeToString(f.type))}</code></td></tr>`;
    });
    returnsHtml = `<div class="section-label">Returns</div>
<table><tr><th>Name</th><th>Type</th></tr>
${rows.join(`
`)}
</table>`;
  }
  let headerHtml = "";
  if (hasHeader && method.headerSchema && method.headerSchema.fields.length > 0) {
    const rows = method.headerSchema.fields.map((f) => {
      return `<tr><td><code>${escapeHtml(f.name)}</code></td><td><code>${escapeHtml(arrowTypeToString(f.type))}</code></td></tr>`;
    });
    headerHtml = `<div class="section-label">Stream Header</div>
<table><tr><th>Name</th><th>Type</th></tr>
${rows.join(`
`)}
</table>`;
  }
  const docHtml = method.doc ? `<p class="docstring">${escapeHtml(method.doc)}</p>` : "";
  return `<div class="card">
<div class="card-header">
<span class="method-name">${name}</span>
${badges.join(`
`)}
</div>
${docHtml}
${paramsHtml}
${returnsHtml}
${headerHtml}
</div>`;
}
function buildDescribePage(protocolName, serverId, methods, repoUrl) {
  const sortedMethods = [...methods.entries()].filter(([name]) => name !== "__describe__").sort(([a], [b]) => a.localeCompare(b));
  const cards = sortedMethods.map(([, method]) => buildMethodCard(method)).join(`
`);
  const repoLink = repoUrl ? ` &middot; <a href="${escapeHtml(repoUrl)}">Source</a>` : "";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(protocolName)} API Reference — vgi-rpc</title>
${FONTS}
<style>
body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 900px;
       margin: 0 auto; padding: 40px 20px 0; color: #2c2c1e; background: #faf8f0; }
.header { text-align: center; margin-bottom: 40px; }
.header .logo img { width: 80px; height: 80px; border-radius: 50%;
                     box-shadow: 0 3px 16px rgba(0,0,0,0.10); }
.header h1 { margin-bottom: 4px; color: #2d5016; font-weight: 700; }
.header .subtitle { color: #6b6b5a; font-size: 1.1em; margin-top: 0; }
.header .meta { color: #6b6b5a; font-size: 0.9em; }
.header .meta a { color: #2d5016; font-weight: 600; }
.header .meta a:hover { color: #4a7c23; }
code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
        padding: 2px 6px; border-radius: 3px; font-size: 0.85em; color: #2c2c1e; }
a { color: #2d5016; text-decoration: none; }
a:hover { color: #4a7c23; }
.card { border: 1px solid #f0ece0; border-radius: 8px; padding: 20px;
         margin-bottom: 16px; background: #fff; }
.card:hover { border-color: #c8a43a; }
.card-header { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
.method-name { font-family: 'JetBrains Mono', monospace; font-size: 1.1em; font-weight: 600;
                color: #2d5016; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
          font-size: 0.75em; font-weight: 600; text-transform: uppercase;
          letter-spacing: 0.03em; }
.badge-unary { background: #e8f5e0; color: #2d5016; }
.badge-stream { background: #e0ecf5; color: #1a4a6b; }
.badge-exchange { background: #f5e6f0; color: #6b234a; }
.badge-producer { background: #e0f0f5; color: #1a5a6b; }
.badge-header { background: #f5eee0; color: #6b4423; }
.docstring { color: #6b6b5a; font-size: 0.9em; margin-top: 0; }
table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
th { text-align: left; padding: 8px 10px; background: #f0ece0; color: #2c2c1e;
      font-weight: 600; border-bottom: 2px solid #e0dcd0; }
td { padding: 8px 10px; border-bottom: 1px solid #f0ece0; }
td code { font-size: 0.85em; }
.no-params { color: #6b6b5a; font-style: italic; font-size: 0.9em; }
.section-label { font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                  letter-spacing: 0.05em; color: #6b6b5a; margin-top: 14px;
                  margin-bottom: 6px; }
footer { text-align: center; margin-top: 48px; padding: 20px 0;
          border-top: 1px solid #f0ece0; color: #6b6b5a; font-size: 0.85em; }
footer a { color: #2d5016; font-weight: 600; }
footer a:hover { color: #4a7c23; }
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <img src="${LOGO_URL}" alt="vgi-rpc logo">
  </div>
  <h1>${escapeHtml(protocolName)}</h1>
  <p class="subtitle">API Reference</p>
  <p class="meta">Powered by <code>vgi-rpc</code> (TypeScript) &middot; server <code>${escapeHtml(serverId)}</code>${repoLink}</p>
</div>
${cards}
<footer>
  <a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
  &middot;
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>`;
}

// src/http/oauth-pkce.ts
var _NODE_CRYPTO_MOD = "node:crypto";
function _crypto() {
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req) {
    throw new Error("OAuth PKCE requires Node.js or Bun (node:crypto).");
  }
  return req(_NODE_CRYPTO_MOD);
}
var createHash = (algo) => _crypto().createHash(algo);
var createHmac = (algo, key) => _crypto().createHmac(algo, key);
var randomBytes3 = (n) => _crypto().randomBytes(n);
var timingSafeEqual = (a, b) => _crypto().timingSafeEqual(a, b);
var SESSION_COOKIE_NAME = "_vgi_oauth_session";
var AUTH_COOKIE_NAME = "_vgi_auth";
var IDENTITY_COOKIE_NAME = "_vgi_identity";
var IDENTITY_CLAIMS = ["sub", "email", "preferred_username", "name", "picture"];
var SESSION_COOKIE_VERSION = 4;
var SESSION_MAX_AGE = 600;
var AUTH_COOKIE_DEFAULT_MAX_AGE = 3600;
var MAX_ORIGINAL_URL_LEN = 2048;
var HMAC_LEN = 32;
var DEFAULT_ALLOWED_RETURN_ORIGINS = new Set(["https://cupola.query-farm.services"]);
function generateCodeVerifier() {
  return randomBytes3(32).toString("base64url");
}
function generateCodeChallenge(verifier) {
  const digest = createHash("sha256").update(verifier, "ascii").digest();
  return digest.toString("base64url");
}
function generateStateNonce() {
  return randomBytes3(24).toString("base64url");
}
function deriveSessionKey(signingKey) {
  return createHmac("sha256", signingKey).update("oauth-pkce-session").digest();
}
function b64urlEncode(buf) {
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_");
}
function b64urlDecode(s) {
  const standard = s.replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(standard, "base64");
}
function packOAuthCookie(codeVerifier, stateNonce, originalUrl, sessionKey, createdAt, returnTo) {
  const now = createdAt ?? Math.floor(Date.now() / 1000);
  const cvBytes = Buffer.from(codeVerifier, "utf-8");
  const stateBytes = Buffer.from(stateNonce, "utf-8");
  const urlBytes = Buffer.from(originalUrl, "utf-8");
  const rtBytes = Buffer.from(returnTo ?? "", "utf-8");
  const payloadLen = 1 + 8 + 2 + cvBytes.length + 2 + stateBytes.length + 2 + urlBytes.length + 2 + rtBytes.length;
  const payload = Buffer.alloc(payloadLen);
  let offset = 0;
  payload.writeUInt8(SESSION_COOKIE_VERSION, offset);
  offset += 1;
  payload.writeBigUInt64LE(BigInt(now), offset);
  offset += 8;
  payload.writeUInt16LE(cvBytes.length, offset);
  offset += 2;
  cvBytes.copy(payload, offset);
  offset += cvBytes.length;
  payload.writeUInt16LE(stateBytes.length, offset);
  offset += 2;
  stateBytes.copy(payload, offset);
  offset += stateBytes.length;
  payload.writeUInt16LE(urlBytes.length, offset);
  offset += 2;
  urlBytes.copy(payload, offset);
  offset += urlBytes.length;
  payload.writeUInt16LE(rtBytes.length, offset);
  offset += 2;
  rtBytes.copy(payload, offset);
  const mac = createHmac("sha256", sessionKey).update(payload).digest();
  return b64urlEncode(Buffer.concat([payload, mac]));
}
function unpackOAuthCookie(cookieValue, sessionKey, maxAge = SESSION_MAX_AGE) {
  let raw;
  try {
    raw = b64urlDecode(cookieValue);
  } catch {
    throw new Error("Malformed session cookie");
  }
  if (raw.length < 49) {
    throw new Error("Session cookie too short");
  }
  const payload = raw.subarray(0, raw.length - HMAC_LEN);
  const receivedMac = raw.subarray(raw.length - HMAC_LEN);
  const expectedMac = createHmac("sha256", sessionKey).update(payload).digest();
  if (!timingSafeEqual(receivedMac, expectedMac)) {
    throw new Error("Session cookie signature mismatch");
  }
  const version = payload.readUInt8(0);
  if (version !== SESSION_COOKIE_VERSION) {
    throw new Error(`Unexpected session cookie version: ${version}`);
  }
  const createdAt = Number(payload.readBigUInt64LE(1));
  if (maxAge > 0) {
    const age = Math.floor(Date.now() / 1000) - createdAt;
    if (age < 0 || age > maxAge) {
      throw new Error(`Session cookie expired (age=${age}s, max=${maxAge}s)`);
    }
  }
  let pos = 9;
  const cvLen = payload.readUInt16LE(pos);
  pos += 2;
  const codeVerifier = payload.subarray(pos, pos + cvLen).toString("utf-8");
  pos += cvLen;
  const stateLen = payload.readUInt16LE(pos);
  pos += 2;
  const stateNonce = payload.subarray(pos, pos + stateLen).toString("utf-8");
  pos += stateLen;
  const urlLen = payload.readUInt16LE(pos);
  pos += 2;
  const originalUrl = payload.subarray(pos, pos + urlLen).toString("utf-8");
  pos += urlLen;
  const rtLen = payload.readUInt16LE(pos);
  pos += 2;
  const returnTo = payload.subarray(pos, pos + rtLen).toString("utf-8");
  return { codeVerifier, stateNonce, originalUrl, returnTo };
}
function createOidcDiscovery(issuer) {
  let cached = null;
  return function discover() {
    if (cached)
      return cached;
    const url = `${issuer.replace(/\/+$/, "")}/.well-known/openid-configuration`;
    cached = fetch(url, { signal: AbortSignal.timeout(1e4) }).then(async (resp) => {
      if (!resp.ok)
        throw new Error(`OIDC discovery HTTP ${resp.status}`);
      const data = await resp.json();
      return {
        authorizationEndpoint: data.authorization_endpoint,
        tokenEndpoint: data.token_endpoint
      };
    }).catch(() => {
      cached = null;
      return null;
    });
    return cached;
  };
}
async function exchangeCodeForToken(tokenEndpoint, code, redirectUri, codeVerifier, clientId, clientSecret, useIdToken) {
  const params = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri,
    code_verifier: codeVerifier,
    client_id: clientId
  });
  if (clientSecret) {
    params.set("client_secret", clientSecret);
  }
  let body;
  try {
    const resp = await fetch(tokenEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params.toString(),
      signal: AbortSignal.timeout(15000)
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`HTTP ${resp.status}: ${text}`);
    }
    body = await resp.json();
  } catch (err2) {
    throw new Error(`Token exchange failed: ${err2.message ?? err2}`);
  }
  const refreshToken = body.refresh_token ?? null;
  const idToken = body.id_token ?? null;
  if (useIdToken) {
    const token2 = body.id_token;
    if (!token2)
      throw new Error("Token response missing id_token");
    try {
      const parts = token2.split(".");
      if (parts.length >= 2) {
        const padding = 4 - parts[1].length % 4;
        const payloadJson = Buffer.from(parts[1] + "=".repeat(padding % 4), "base64").toString("utf-8");
        const claims = JSON.parse(payloadJson);
        if (claims.exp != null) {
          const maxAge = Math.max(Number(claims.exp) - Math.floor(Date.now() / 1000), 60);
          return { token: token2, maxAge, refreshToken, idToken };
        }
      }
    } catch {}
    return { token: token2, maxAge: AUTH_COOKIE_DEFAULT_MAX_AGE, refreshToken, idToken };
  }
  const token = body.access_token;
  if (!token)
    throw new Error("Token response missing access_token");
  const expiresIn = body.expires_in ?? AUTH_COOKIE_DEFAULT_MAX_AGE;
  return { token, maxAge: Number(expiresIn), refreshToken, idToken };
}
function validateOriginalUrl(url, prefix) {
  let u = url;
  if (u.length > MAX_ORIGINAL_URL_LEN) {
    u = u.slice(0, MAX_ORIGINAL_URL_LEN);
  }
  try {
    const parsed = new URL(u, "http://dummy");
    if (u.startsWith("http://") || u.startsWith("https://") || u.startsWith("//")) {
      return prefix || "/";
    }
    if (parsed.hostname !== "dummy") {
      return prefix || "/";
    }
  } catch {
    return prefix || "/";
  }
  if (prefix && !u.startsWith(prefix)) {
    return prefix || "/";
  }
  return u;
}
function isLocalhost(hostname) {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}
function validateReturnTo(url, allowedOrigins) {
  const origins = allowedOrigins ?? DEFAULT_ALLOWED_RETURN_ORIGINS;
  if (!url || url.length > 2048)
    return "";
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    return "";
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:")
    return "";
  if (!parsed.hostname)
    return "";
  if (isLocalhost(parsed.hostname) && parsed.protocol === "http:")
    return url;
  const origin = `${parsed.protocol}//${parsed.hostname}`;
  if (origins.has(origin))
    return url;
  if (parsed.port) {
    const originWithPort = `${parsed.protocol}//${parsed.hostname}:${parsed.port}`;
    if (origins.has(originWithPort))
      return url;
  }
  return "";
}
function parseCookies(request) {
  const header = request.headers.get("Cookie");
  const map2 = new Map;
  if (!header)
    return map2;
  for (const pair of header.split(";")) {
    const eq = pair.indexOf("=");
    if (eq < 0)
      continue;
    const name = pair.slice(0, eq).trim();
    const value = pair.slice(eq + 1).trim();
    map2.set(name, value);
  }
  return map2;
}
function decodeJwtPayload(token) {
  if (!token)
    return null;
  try {
    const parts = token.split(".");
    if (parts.length < 2)
      return null;
    const padding = (4 - parts[1].length % 4) % 4;
    const json = Buffer.from(parts[1] + "=".repeat(padding), "base64").toString("utf-8");
    const claims = JSON.parse(json);
    return claims && typeof claims === "object" ? claims : null;
  } catch {
    return null;
  }
}
function identityCookieValue(idToken) {
  const claims = decodeJwtPayload(idToken);
  if (!claims)
    return null;
  const ident = {};
  for (const key of IDENTITY_CLAIMS) {
    if (claims[key] != null)
      ident[key] = claims[key];
  }
  if (Object.keys(ident).length === 0)
    return null;
  return Buffer.from(JSON.stringify(ident), "utf-8").toString("base64url");
}
function buildSetCookieHeader(name, value, options) {
  let cookie = `${name}=${value}`;
  if (options.maxAge !== undefined)
    cookie += `; Max-Age=${options.maxAge}`;
  if (options.path)
    cookie += `; Path=${options.path}`;
  if (options.secure)
    cookie += "; Secure";
  if (options.httpOnly)
    cookie += "; HttpOnly";
  if (options.sameSite)
    cookie += `; SameSite=${options.sameSite}`;
  return cookie;
}
function escapeHtml2(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function buildOAuthErrorPage(message, detail, retryUrl) {
  const detailHtml = detail ? `<div class="detail">${escapeHtml2(detail)}</div>` : "";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Authentication Error — vgi-rpc</title>
${FONTS}
${ERROR_PAGE_STYLE}
</head>
<body>
<div class="logo">
  <img src="${LOGO_URL}" alt="vgi-rpc logo">
</div>
<h1>Authentication Error</h1>
<p>${escapeHtml2(message)}</p>
${detailHtml}
<p><a href="${escapeHtml2(retryUrl)}">Try again</a></p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>`;
}
function cookieAuthenticate(innerAuth, cookieName = AUTH_COOKIE_NAME) {
  return async function authenticate(request) {
    const cookies = parseCookies(request);
    const token = cookies.get(cookieName);
    if (!token) {
      throw new Error("No auth cookie");
    }
    const newHeaders = new Headers(request.headers);
    newHeaders.set("Authorization", `Bearer ${token}`);
    const newRequest = new Request(request.url, {
      method: request.method,
      headers: newHeaders
    });
    return innerAuth(newRequest);
  };
}
function resolvePkceScope(scopesSupported, optionsScope) {
  if (scopesSupported && scopesSupported.length > 0) {
    return scopesSupported.join(" ");
  }
  return optionsScope;
}
function configureOAuthPkce(opts, innerAuth) {
  const sessionKey = deriveSessionKey(opts.signingKey);
  const oidcDiscovery = createOidcDiscovery(opts.issuer);
  return {
    sessionKey,
    oidcDiscovery,
    clientId: opts.clientId,
    clientSecret: opts.clientSecret,
    useIdToken: opts.useIdToken ?? false,
    prefix: opts.prefix,
    secureCookie: opts.secureCookie,
    redirectUri: opts.redirectUri,
    scope: opts.scope ?? "openid email",
    allowedReturnOrigins: opts.allowedReturnOrigins ?? DEFAULT_ALLOWED_RETURN_ORIGINS,
    cookieAuthenticate: cookieAuthenticate(innerAuth)
  };
}
var ALLOWED_TOKEN_GRANT_TYPES = new Set(["authorization_code", "refresh_token"]);
function isLocalhostHost(hostname) {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}
function setProxyCors(headers, request, config) {
  headers.append("Vary", "Origin");
  const origin = request.headers.get("Origin");
  if (!origin)
    return;
  let parsed;
  try {
    parsed = new URL(origin);
  } catch {
    return;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:")
    return;
  if (!parsed.hostname)
    return;
  if (isLocalhostHost(parsed.hostname) && parsed.protocol === "http:") {
    headers.set("Access-Control-Allow-Origin", origin);
    return;
  }
  if (config.allowedReturnOrigins.has(origin)) {
    headers.set("Access-Control-Allow-Origin", origin);
  }
}
function jsonErrorResponse(headers, status, error, description) {
  headers.set("Content-Type", "application/json");
  return new Response(JSON.stringify({ error, error_description: description }), { status, headers });
}
async function handleOAuthTokenProxy(request, config) {
  const headers = new Headers;
  setProxyCors(headers, request, config);
  if (request.method === "OPTIONS") {
    headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "Content-Type");
    headers.set("Access-Control-Max-Age", "7200");
    return new Response(null, { status: 204, headers });
  }
  if (request.method !== "POST") {
    return new Response(null, { status: 405, headers });
  }
  const ctype = (request.headers.get("Content-Type") ?? "").split(";")[0].trim().toLowerCase();
  if (ctype !== "application/x-www-form-urlencoded") {
    return jsonErrorResponse(headers, 415, "invalid_request", "Content-Type must be application/x-www-form-urlencoded");
  }
  let raw;
  try {
    raw = await request.text();
  } catch {
    return jsonErrorResponse(headers, 400, "invalid_request", "Could not read request body");
  }
  let form;
  try {
    form = new URLSearchParams(raw);
  } catch {
    return jsonErrorResponse(headers, 400, "invalid_request", "Could not parse form body");
  }
  const grantType = form.get("grant_type") ?? "";
  if (!ALLOWED_TOKEN_GRANT_TYPES.has(grantType)) {
    return jsonErrorResponse(headers, 400, "unsupported_grant_type", "grant_type must be authorization_code or refresh_token");
  }
  const submittedClientId = form.get("client_id");
  if (submittedClientId && submittedClientId !== config.clientId) {
    return jsonErrorResponse(headers, 400, "invalid_client", "client_id does not match the configured client");
  }
  const endpoints = await config.oidcDiscovery();
  if (!endpoints) {
    return jsonErrorResponse(headers, 502, "server_error", "Authorization server discovery failed");
  }
  const upstream = new URLSearchParams;
  upstream.set("grant_type", grantType);
  upstream.set("client_id", config.clientId);
  if (config.clientSecret) {
    upstream.set("client_secret", config.clientSecret);
  }
  for (const key of ["code", "code_verifier", "redirect_uri", "refresh_token", "scope"]) {
    const value = form.get(key);
    if (value !== null && value !== "") {
      upstream.set(key, value);
    }
  }
  let upstreamResp;
  try {
    upstreamResp = await fetch(endpoints.tokenEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: upstream.toString(),
      signal: AbortSignal.timeout(15000)
    });
  } catch (err2) {
    return jsonErrorResponse(headers, 502, "server_error", `Upstream token endpoint failed: ${err2?.message ?? err2}`);
  }
  const body = new Uint8Array(await upstreamResp.arrayBuffer());
  const ct = upstreamResp.headers.get("content-type") ?? "application/json";
  headers.set("Content-Type", ct);
  return new Response(body, { status: upstreamResp.status, headers });
}
async function handleOAuthCallback(request, config) {
  const url = new URL(request.url);
  const retryUrl = config.prefix || "/";
  function errorResponse(status, message, detail) {
    return new Response(buildOAuthErrorPage(message, detail, retryUrl), {
      status,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
  const error = url.searchParams.get("error");
  if (error) {
    const errorDesc = url.searchParams.get("error_description") ?? error;
    return errorResponse(400, "The authorization server returned an error.", errorDesc);
  }
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  if (!code || !state) {
    return errorResponse(400, "Missing authorization code or state parameter.", null);
  }
  const cookies = parseCookies(request);
  const sessionCookie = cookies.get(SESSION_COOKIE_NAME);
  if (!sessionCookie) {
    return errorResponse(400, "Session cookie missing or expired. Please try again.", null);
  }
  let unpacked;
  try {
    unpacked = unpackOAuthCookie(sessionCookie, config.sessionKey);
  } catch {
    return errorResponse(400, "Session expired or invalid. Please try again.", null);
  }
  const stateA = Buffer.from(state, "utf-8");
  const stateB = Buffer.from(unpacked.stateNonce, "utf-8");
  if (stateA.length !== stateB.length || !timingSafeEqual(stateA, stateB)) {
    return errorResponse(400, "State mismatch — possible CSRF. Please try again.", null);
  }
  const endpoints = await config.oidcDiscovery();
  if (!endpoints) {
    return errorResponse(502, "Could not reach the authorization server.", "OIDC discovery failed.");
  }
  let result;
  try {
    result = await exchangeCodeForToken(endpoints.tokenEndpoint, code, config.redirectUri, unpacked.codeVerifier, config.clientId, config.clientSecret, config.useIdToken);
  } catch (err2) {
    return errorResponse(502, "Token exchange with the authorization server failed.", String(err2.message ?? err2));
  }
  const clearSessionCookie = buildSetCookieHeader(SESSION_COOKIE_NAME, "", {
    maxAge: 0,
    path: `${config.prefix}/_oauth/`,
    secure: config.secureCookie,
    httpOnly: true,
    sameSite: "Lax"
  });
  if (unpacked.returnTo) {
    const separator = unpacked.returnTo.includes("#") ? "&" : "#";
    const fragmentParts = [`token=${result.token}`];
    if (result.refreshToken) {
      fragmentParts.push(`refresh_token=${encodeURIComponent(result.refreshToken)}`);
    }
    fragmentParts.push(`token_endpoint=${encodeURIComponent(endpoints.tokenEndpoint)}`);
    fragmentParts.push(`client_id=${encodeURIComponent(config.clientId)}`);
    if (config.clientSecret) {
      fragmentParts.push(`client_secret=${encodeURIComponent(config.clientSecret)}`);
    }
    if (config.useIdToken) {
      fragmentParts.push("use_id_token=true");
    }
    const redirectUrl = `${unpacked.returnTo}${separator}${fragmentParts.join("&")}`;
    return new Response(null, {
      status: 302,
      headers: {
        Location: redirectUrl,
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Content-Type": "text/html; charset=utf-8",
        "Set-Cookie": clearSessionCookie
      }
    });
  }
  const originalUrl = validateOriginalUrl(unpacked.originalUrl, config.prefix);
  const cookiePath = config.prefix || "/";
  const authCookie = buildSetCookieHeader(AUTH_COOKIE_NAME, result.token, {
    maxAge: result.maxAge,
    path: cookiePath,
    secure: config.secureCookie,
    httpOnly: false,
    sameSite: "Lax"
  });
  const headers = new Headers;
  headers.set("Location", originalUrl);
  headers.set("Cache-Control", "no-cache, no-store, must-revalidate");
  headers.set("Content-Type", "text/html; charset=utf-8");
  headers.append("Set-Cookie", authCookie);
  headers.append("Set-Cookie", clearSessionCookie);
  const identity = identityCookieValue(result.idToken ?? (config.useIdToken ? result.token : null));
  if (identity !== null) {
    headers.append("Set-Cookie", buildSetCookieHeader(IDENTITY_COOKIE_NAME, identity, {
      maxAge: result.maxAge,
      path: cookiePath,
      secure: config.secureCookie,
      httpOnly: false,
      sameSite: "Lax"
    }));
  }
  return new Response(null, { status: 302, headers });
}
function handleOAuthLogout(_request, config) {
  const cookiePath = config.prefix || "/";
  const clearAuthCookie = buildSetCookieHeader(AUTH_COOKIE_NAME, "", {
    maxAge: 0,
    path: cookiePath,
    secure: config.secureCookie,
    httpOnly: false
  });
  const clearIdentityCookie = buildSetCookieHeader(IDENTITY_COOKIE_NAME, "", {
    maxAge: 0,
    path: cookiePath,
    secure: config.secureCookie,
    httpOnly: false
  });
  const headers = new Headers;
  headers.set("Location", config.prefix || "/");
  headers.append("Set-Cookie", clearAuthCookie);
  headers.append("Set-Cookie", clearIdentityCookie);
  return new Response(null, { status: 302, headers });
}
async function handleBrowserGetRedirect(request, config) {
  const accept = request.headers.get("Accept") ?? "";
  if (!accept.includes("text/html"))
    return null;
  const endpoints = await config.oidcDiscovery();
  if (!endpoints)
    return null;
  const url = new URL(request.url);
  const codeVerifier = generateCodeVerifier();
  const codeChallenge = generateCodeChallenge(codeVerifier);
  const stateNonce = generateStateNonce();
  let originalUrl = url.pathname;
  if (url.search) {
    originalUrl = `${originalUrl}${url.search}`;
  }
  originalUrl = validateOriginalUrl(originalUrl, config.prefix);
  const returnTo = validateReturnTo(url.searchParams.get("_vgi_return_to") ?? "", config.allowedReturnOrigins);
  const cookieValue = packOAuthCookie(codeVerifier, stateNonce, originalUrl, config.sessionKey, undefined, returnTo);
  const authParams = new URLSearchParams({
    response_type: "code",
    client_id: config.clientId,
    redirect_uri: config.redirectUri,
    code_challenge: codeChallenge,
    code_challenge_method: "S256",
    state: stateNonce,
    scope: config.scope
  });
  if (returnTo) {
    authParams.set("access_type", "offline");
    authParams.set("prompt", "consent");
  }
  const authUrl = `${endpoints.authorizationEndpoint}?${authParams.toString()}`;
  const sessionCookie = buildSetCookieHeader(SESSION_COOKIE_NAME, cookieValue, {
    maxAge: SESSION_MAX_AGE,
    path: `${config.prefix}/_oauth/`,
    secure: config.secureCookie,
    httpOnly: true,
    sameSite: "Lax"
  });
  return new Response(null, {
    status: 302,
    headers: {
      Location: authUrl,
      "Cache-Control": "no-cache, no-store, must-revalidate",
      "Content-Type": "text/html; charset=utf-8",
      "Set-Cookie": sessionCookie
    }
  });
}
function handleEarlyReturnTo(request, config) {
  const url = new URL(request.url);
  const returnTo = validateReturnTo(url.searchParams.get("_vgi_return_to") ?? "", config.allowedReturnOrigins);
  if (!returnTo)
    return null;
  const cookies = parseCookies(request);
  const token = cookies.get(AUTH_COOKIE_NAME);
  if (!token)
    return null;
  try {
    const parts = token.split(".");
    if (parts.length >= 2) {
      const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf-8"));
      if (typeof payload.exp === "number" && payload.exp <= Math.floor(Date.now() / 1000)) {
        return null;
      }
    }
  } catch {}
  const separator = returnTo.includes("#") ? "&" : "#";
  const fragmentParams = [`token=${token}`];
  const redirectUrl = `${returnTo}${separator}${fragmentParams.join("&")}`;
  return new Response(null, {
    status: 302,
    headers: {
      Location: redirectUrl,
      "Cache-Control": "no-cache, no-store, must-revalidate"
    }
  });
}

// src/http/proof.ts
var PROOF_HEADER = "VGI-Proxy-Proof";
var PROOF_REQUIRED_HEADER = "VGI-Proxy-Proof-Required";
var _enc = new TextEncoder;
class NonceCache {
  ttlSeconds;
  capacity;
  entries = new Map;
  constructor(ttlSeconds, capacity) {
    this.ttlSeconds = ttlSeconds;
    this.capacity = capacity;
  }
  checkAndAdd(nonce, now) {
    for (const [key, expires] of this.entries) {
      if (expires > now)
        break;
      this.entries.delete(key);
    }
    if (this.entries.has(nonce))
      return false;
    while (this.entries.size >= this.capacity) {
      const oldest = this.entries.keys().next();
      if (oldest.done)
        break;
      this.entries.delete(oldest.value);
    }
    this.entries.set(nonce, now + this.ttlSeconds);
    return true;
  }
  get size() {
    return this.entries.size;
  }
}

// src/http/sticky.ts
var _UTF82 = new TextEncoder;
var _UTF8DEC = new TextDecoder("utf-8", { fatal: false });
var TOKEN_VERSION2 = 1;
var SESSION_ID_LEN = 12;
var PREFIX_LEN = 8 + 1;
var SUFFIX_LEN = 8;
function base64UrlEncode(bytes) {
  let s = "";
  for (let i = 0;i < bytes.length; i += 32768) {
    s += String.fromCharCode(...bytes.subarray(i, i + 32768));
  }
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function base64UrlDecode(s) {
  let b64 = s.replace(/-/g, "+").replace(/_/g, "/");
  const pad = b64.length % 4;
  if (pad === 2)
    b64 += "==";
  else if (pad === 3)
    b64 += "=";
  else if (pad === 1)
    throw new Error("invalid base64url length");
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0;i < bin.length; i++)
    out[i] = bin.charCodeAt(i);
  return out;
}
function sealSessionToken(serverId, sessionId, expiresAt, tokenKey, aad, now) {
  if (sessionId.length !== SESSION_ID_LEN) {
    throw new Error(`session_id must be ${SESSION_ID_LEN} bytes, got ${sessionId.length}`);
  }
  const serverIdBytes = _UTF82.encode(serverId);
  if (serverIdBytes.length > 255) {
    throw new Error(`server_id too long (${serverIdBytes.length} bytes); max 255`);
  }
  const plaintext = new Uint8Array(PREFIX_LEN + serverIdBytes.length + SESSION_ID_LEN + SUFFIX_LEN);
  const view = new DataView(plaintext.buffer);
  let offset = 0;
  view.setBigUint64(offset, BigInt(now ?? Math.floor(Date.now() / 1000)), true);
  offset += 8;
  plaintext[offset] = serverIdBytes.length;
  offset += 1;
  plaintext.set(serverIdBytes, offset);
  offset += serverIdBytes.length;
  plaintext.set(sessionId, offset);
  offset += SESSION_ID_LEN;
  view.setBigUint64(offset, BigInt(expiresAt), true);
  const sealed = sealBytes(plaintext, tokenKey, { aad, version: TOKEN_VERSION2 });
  return base64UrlEncode(sealed);
}
function openSessionToken(token, tokenKey, aad) {
  let raw;
  try {
    raw = base64UrlDecode(token);
  } catch {
    throw new SessionLostError("malformed session token");
  }
  let plaintext;
  try {
    plaintext = openBytes(raw, tokenKey, { aad, version: TOKEN_VERSION2 });
  } catch (err2) {
    if (err2 instanceof SealError) {
      throw new SessionLostError("session token verification failed");
    }
    throw err2;
  }
  if (plaintext.length < PREFIX_LEN) {
    throw new SessionLostError("malformed session token");
  }
  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  const serverIdLen = plaintext[8];
  const sidPos = PREFIX_LEN + serverIdLen;
  const endPos = sidPos + SESSION_ID_LEN + SUFFIX_LEN;
  if (plaintext.length !== endPos) {
    throw new SessionLostError("malformed session token");
  }
  const serverId = _UTF8DEC.decode(plaintext.subarray(PREFIX_LEN, sidPos));
  const sessionId = new Uint8Array(SESSION_ID_LEN);
  sessionId.set(plaintext.subarray(sidPos, sidPos + SESSION_ID_LEN));
  const expiresAt = Number(view.getBigUint64(sidPos + SESSION_ID_LEN, true));
  return { serverId, sessionId, expiresAt };
}

class AsyncMutex {
  locked = false;
  waiters = [];
  async acquire() {
    if (!this.locked) {
      this.locked = true;
      return () => this.release();
    }
    await new Promise((resolve) => this.waiters.push(resolve));
    this.locked = true;
    return () => this.release();
  }
  release() {
    const next = this.waiters.shift();
    if (next) {
      next();
    } else {
      this.locked = false;
    }
  }
}
function sessionPrincipalKey(authenticated, domain, principal) {
  if (!authenticated)
    return "\x00anonymous";
  return `${domain ?? ""}\x00${principal ?? ""}`;
}
function sessionIdHex(sessionId) {
  let s = "";
  for (let i = 0;i < sessionId.length; i++)
    s += sessionId[i].toString(16).padStart(2, "0");
  return s;
}
function bytesEqual(a, b) {
  if (a.length !== b.length)
    return false;
  for (let i = 0;i < a.length; i++)
    if (a[i] !== b[i])
      return false;
  return true;
}

class SessionRegistry {
  defaultTtl;
  entries = new Map;
  _draining = false;
  constructor(defaultTtl) {
    this.defaultTtl = defaultTtl;
  }
  get draining() {
    return this._draining;
  }
  setDraining(value) {
    this._draining = value;
  }
  open(state, ttl, principalKey) {
    if (this._draining) {
      throw new ServerDrainingError("server is draining — new sessions are rejected");
    }
    const effective = ttl ?? this.defaultTtl;
    const expiresAt = Math.floor(Date.now() / 1000) + effective;
    const sessionId = randomBytes(SESSION_ID_LEN);
    const key = sessionIdHex(sessionId);
    this.entries.set(key, {
      id: sessionId,
      entry: { state, expiresAt, principalKey, lock: new AsyncMutex }
    });
    return { sessionId, expiresAt };
  }
  get(sessionId, principalKey) {
    const key = sessionIdHex(sessionId);
    const slot = this.entries.get(key);
    if (!slot)
      return null;
    if (!bytesEqual(slot.id, sessionId))
      return null;
    const now = Math.floor(Date.now() / 1000);
    if (slot.entry.expiresAt < now) {
      this.entries.delete(key);
      closeStateSuppressed(slot.entry.state);
      return null;
    }
    if (slot.entry.principalKey !== principalKey)
      return null;
    return slot.entry;
  }
  close(sessionId) {
    const key = sessionIdHex(sessionId);
    const slot = this.entries.get(key);
    if (!slot || !bytesEqual(slot.id, sessionId))
      return false;
    this.entries.delete(key);
    closeStateSuppressed(slot.entry.state);
    return true;
  }
  drainExpired(now) {
    const cutoff = now ?? Math.floor(Date.now() / 1000);
    let count = 0;
    for (const [key, slot] of this.entries) {
      if (slot.entry.expiresAt < cutoff) {
        this.entries.delete(key);
        closeStateSuppressed(slot.entry.state);
        count++;
      }
    }
    return count;
  }
  shutdown() {
    const slots = Array.from(this.entries.values());
    this.entries.clear();
    for (const slot of slots)
      closeStateSuppressed(slot.entry.state);
  }
  get size() {
    return this.entries.size;
  }
}
function closeStateSuppressed(state) {
  const closer = state?.close;
  if (typeof closer !== "function")
    return;
  try {
    closer.call(state);
  } catch {}
}
function startSessionReaper(registry, tickMs = 1000) {
  const handle = setInterval(() => {
    try {
      registry.drainExpired();
    } catch {}
  }, tickMs);
  handle.unref?.();
  return () => clearInterval(handle);
}
function makeDrainHandle(registry, stopReaper) {
  return {
    drain: () => registry.setDraining(true),
    shutdown: () => {
      stopReaper?.();
      registry.shutdown();
    },
    isDraining: () => registry.draining,
    setDraining: (v) => registry.setDraining(v)
  };
}

// src/http/types.ts
var jsonStateSerializer = {
  serialize(state) {
    return new TextEncoder().encode(JSON.stringify(state, (_key, value) => typeof value === "bigint" ? `__bigint__:${value}` : value));
  },
  deserialize(bytes) {
    return JSON.parse(new TextDecoder().decode(bytes), (_key, value) => typeof value === "string" && value.startsWith("__bigint__:") ? BigInt(value.slice(11)) : value);
  }
};

// src/http/handler.ts
var EMPTY_SCHEMA2 = schema([]);
var EMPTY_COOKIES2 = new Map;
function parseRequestCookies(request) {
  const header = request.headers.get("cookie");
  if (!header)
    return EMPTY_COOKIES2;
  const out = new Map;
  for (const part of header.split(";")) {
    const pair = part.trim();
    if (!pair)
      continue;
    const eq = pair.indexOf("=");
    if (eq < 0)
      continue;
    out.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
  }
  return out;
}
function createHttpHandler(protocol, options) {
  const prefix = (options?.prefix ?? "").replace(/\/+$/, "");
  const tokenKey = options?.tokenKey ?? randomBytes(32);
  const tokenTtl = options?.tokenTtl ?? 3600;
  const corsOrigins = options?.corsOrigins;
  const corsMaxAge = options?.corsMaxAge === undefined ? 7200 : options.corsMaxAge;
  const maxRequestBytes = options?.maxRequestBytes;
  const maxDecompressedRequestBytes = options?.maxDecompressedRequestBytes ?? (options?.maxRequestBytes != null ? options.maxRequestBytes * 16 : undefined);
  const maxStreamResponseBytes = options?.maxStreamResponseBytes;
  const maxResponseBytes = options?.maxResponseBytes;
  const maxExternalizedResponseBytes = options?.maxExternalizedResponseBytes;
  const serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
  let authenticate = options?.authenticate;
  const oauthMetadata = options?.oauthResourceMetadata;
  let pkceConfig = null;
  if (authenticate && oauthMetadata?.clientId) {
    const resourceUrl = new URL(oauthMetadata.resource);
    const secureCookie = resourceUrl.protocol === "https:";
    const redirectUri = `${oauthMetadata.resource.replace(/\/+$/, "")}${prefix}/_oauth/callback`;
    const issuer = oauthMetadata.authorizationServers[0];
    if (issuer) {
      const originalAuth = authenticate;
      pkceConfig = configureOAuthPkce({
        signingKey: tokenKey,
        issuer,
        clientId: oauthMetadata.clientId,
        clientSecret: oauthMetadata.clientSecret,
        useIdToken: oauthMetadata.useIdTokenAsBearer,
        prefix,
        secureCookie,
        redirectUri,
        scope: resolvePkceScope(oauthMetadata.scopesSupported, options?.oauthPkceScope),
        allowedReturnOrigins: options?.allowedReturnOrigins
      }, originalAuth);
      authenticate = chainAuthenticate(originalAuth, pkceConfig.cookieAuthenticate);
    }
  }
  const methods = protocol.getMethods();
  let protocolHashPromise = null;
  function getProtocolHash() {
    if (!protocolHashPromise) {
      protocolHashPromise = buildDescribeBatch(protocol.name, methods, serverId, protocol.protocolVersion || undefined).then(({ metadata }) => metadata.get(PROTOCOL_HASH_KEY) ?? "");
    }
    return protocolHashPromise;
  }
  const protocolVersion = protocol.protocolVersion || options?.protocolVersion || "";
  function enforceProtocolVersion(reqBatchMeta) {
    const parts = protocol.protocolVersionParts;
    if (parts === null)
      return;
    const serverVersion = protocol.protocolVersion;
    const clientVersion = reqBatchMeta?.get(PROTOCOL_VERSION_KEY);
    if (clientVersion === undefined) {
      throw new ProtocolVersionError(`VGI client/worker protocol_version mismatch.
` + `  Client: <not declared>
` + `  Server: ${serverVersion}
` + "  Direction: the client did not send a vgi_rpc.protocol_version " + "metadata key. This is either a vgi-rpc framework bug or a " + "non-VGI client connecting to a VGI worker.");
    }
    let clientParts;
    try {
      clientParts = parseProtocolVersion(clientVersion);
    } catch {
      throw new ProtocolVersionError(`VGI client/worker protocol_version mismatch.
` + `  Client: ${clientVersion}
` + `  Server: ${serverVersion}
` + "  Direction: client sent a malformed protocol_version. " + "Expected canonical semver MAJOR.MINOR.PATCH.");
    }
    if (clientParts[0] === parts[0] && clientParts[1] === parts[1])
      return;
    const clientOlder = clientParts[0] < parts[0] || clientParts[0] === parts[0] && clientParts[1] < parts[1];
    const direction = clientOlder ? `client is too old; upgrade the VGI extension/client to a version supporting protocol_version ${serverVersion}.` : `server is too old; upgrade the VGI worker to a version supporting protocol_version ${clientVersion}.`;
    throw new ProtocolVersionError(`VGI client/worker protocol_version mismatch.
` + `  Client: ${clientVersion}
` + `  Server: ${serverVersion}
` + `  Direction: ${direction}`);
  }
  const compressionLevel = options?.compressionLevel === undefined ? DEFAULT_COMPRESSION_LEVEL : options.compressionLevel;
  const stampCustomContentEncoding = isWorkerd();
  const stateSerializer = options?.stateSerializer ?? jsonStateSerializer;
  const dispatchHook = options?.dispatchHook;
  const onServeStart = options?.onServeStart ?? null;
  let serveStartFired = false;
  let serveStartInFlight = null;
  const transportKind = options?._transportKind ?? "http" /* HTTP */;
  async function notifyTransport(kind) {
    if (serveStartFired)
      return;
    if (serveStartInFlight) {
      await serveStartInFlight;
      return;
    }
    if (!onServeStart) {
      serveStartFired = true;
      return;
    }
    serveStartInFlight = (async () => {
      try {
        await onServeStart(kind);
        serveStartFired = true;
      } finally {
        serveStartInFlight = null;
      }
    })();
    await serveStartInFlight;
  }
  const enableLandingPage = options?.enableLandingPage ?? true;
  const enableDescribePage = options?.enableDescribePage ?? true;
  const enableNotFoundPage = options?.enableNotFoundPage ?? true;
  const displayName = options?.protocolName ?? protocol.name;
  const repoUrl = options?.repositoryUrl ?? null;
  const extraRoutes = options?.extraRoutes ?? null;
  const oauthActive = pkceConfig != null;
  const genericLandingHtml = enableLandingPage ? buildLandingPage(displayName, serverId, enableDescribePage ? `${prefix}/describe` : null, repoUrl) : null;
  const describeHtml = enableDescribePage ? buildDescribePage(displayName, serverId, methods, repoUrl) : null;
  const notFoundHtml = enableNotFoundPage ? buildNotFoundPage(prefix, displayName) : null;
  const externalLocation = options?.externalLocation;
  const uploadUrlProvider = options?.uploadUrlProvider;
  const maxUploadBytes = options?.maxUploadBytes;
  const proxyProofRequired = options?.proxyProofRequired === true;
  const proxyAuthHeaders = [...proxyProofRequired ? [PROOF_HEADER] : [], ...options?.proxyAuthHeaders ?? []];
  const proxyHint = buildProxyHint(proxyAuthHeaders);
  const introspectPath = `${prefix}${INTROSPECT_ENDPOINT}`;
  const introspector = options?.introspectResolver ? createIntrospector({
    resolver: options.introspectResolver,
    principals: options.introspectPrincipals,
    ttlSeconds: options.introspectTtlSeconds,
    rateLimit: options.introspectRateLimit
  }) : null;
  if (!introspector && options?.introspectPrincipals) {
    throw new Error("introspectPrincipals was given without introspectResolver; the endpoint stays " + "disabled, so the allowlist would have no effect. Pass both or neither.");
  }
  const stickyEnabled = options?.enableSticky === true;
  const stickyDefaultTtl = options?.stickyDefaultTtl ?? 300;
  const stickyEchoHeadersArr = stickyEnabled ? Object.entries(options?.stickyEchoHeaders ?? {}) : [];
  const sessionRegistry = stickyEnabled ? new SessionRegistry(stickyDefaultTtl) : null;
  const stopReaper = sessionRegistry ? startSessionReaper(sessionRegistry) : null;
  if (options?._onStickyHandle && sessionRegistry) {
    options._onStickyHandle(makeDrainHandle(sessionRegistry, stopReaper ?? undefined));
  }
  const zstdResponseAvailable = compressionLevel != null && isZstdCompressAvailable();
  function canProduceEncoding(encoding) {
    if (compressionLevel == null)
      return false;
    return encoding === "zstd" ? zstdResponseAvailable : true;
  }
  function canDecodeEncoding(_encoding) {
    return true;
  }
  const supportedEncodings = COMPRESSION_ENCODINGS.filter((e) => canDecodeEncoding(e) && canProduceEncoding(e));
  function addCapabilityHeaders(headers, isOptions = false) {
    headers.set(SUPPORTED_ENCODINGS_HEADER, supportedEncodings.join(", "));
    if (maxRequestBytes != null) {
      headers.set("VGI-Max-Request-Bytes", String(maxRequestBytes));
    }
    if (maxResponseBytes != null) {
      headers.set("VGI-Max-Response-Bytes", String(maxResponseBytes));
    }
    if (maxExternalizedResponseBytes != null) {
      headers.set("VGI-Max-Externalized-Response-Bytes", String(maxExternalizedResponseBytes));
    }
    headers.set("VGI-Externalization-Enabled", externalLocation?.storage ? "true" : "false");
    if (uploadUrlProvider) {
      headers.set("VGI-Upload-URL-Support", "true");
      if (maxUploadBytes != null) {
        headers.set("VGI-Max-Upload-Bytes", String(maxUploadBytes));
      }
    }
    if (proxyProofRequired) {
      headers.set(PROOF_REQUIRED_HEADER, "true");
    }
    if (introspector) {
      headers.set(INTROSPECT_ENABLED_HEADER, "true");
    }
    if (stickyEnabled) {
      headers.set(STICKY_ENABLED_HEADER, "true");
      headers.set(STICKY_DEFAULT_TTL_HEADER, String(Math.floor(stickyDefaultTtl)));
      if (stickyEchoHeadersArr.length > 0) {
        headers.set(STICKY_ECHO_HEADERS_HEADER, stickyEchoHeadersArr.map(([k]) => k).join(", "));
      }
    }
    if (isOptions && (maxRequestBytes != null || uploadUrlProvider || stickyEnabled)) {
      if (!headers.has("Cache-Control")) {
        headers.set("Cache-Control", "public, max-age=300");
      }
    }
  }
  const baseCtx = {
    tokenKey,
    tokenTtl,
    serverId,
    maxStreamResponseBytes,
    maxResponseBytes,
    maxExternalizedResponseBytes,
    stateSerializer,
    externalLocation,
    kind: transportKind,
    callStateCacheEntries: options?.callStateCacheEntries
  };
  const corsExposeHeaders = [
    "WWW-Authenticate",
    REQUEST_ID_HEADER,
    VGI_CONTENT_ENCODING_HEADER,
    RPC_ERROR_HEADER,
    "VGI-Max-Response-Bytes",
    "VGI-Max-Externalized-Response-Bytes",
    "VGI-Externalization-Enabled",
    SUPPORTED_ENCODINGS_HEADER,
    ...maxRequestBytes != null ? ["VGI-Max-Request-Bytes"] : [],
    ...uploadUrlProvider ? ["VGI-Upload-URL-Support", ...maxUploadBytes != null ? ["VGI-Max-Upload-Bytes"] : []] : [],
    ...proxyProofRequired ? [PROOF_REQUIRED_HEADER] : [],
    ...introspector ? [INTROSPECT_ENABLED_HEADER] : [],
    ...stickyEnabled ? [
      STICKY_ENABLED_HEADER,
      STICKY_DEFAULT_TTL_HEADER,
      ...stickyEchoHeadersArr.length > 0 ? [STICKY_ECHO_HEADERS_HEADER] : [],
      SESSION_HEADER,
      SESSION_CLOSE_HEADER,
      ...stickyEchoHeadersArr.map(([name]) => `${ECHO_HEADER_PREFIX}${name}`)
    ] : [],
    AUTH_REASON_HEADER,
    ...proxyHint ? [AUTH_PROXY_REQUIRED_HEADER] : []
  ].join(", ");
  function addCorsHeaders(headers, isOptions = false, requestedHeaders) {
    if (corsOrigins) {
      headers.set("Access-Control-Allow-Origin", corsOrigins);
      headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
      headers.set("Access-Control-Allow-Headers", requestedHeaders && requestedHeaders.length > 0 ? requestedHeaders : "Content-Type, Authorization");
      headers.set("Access-Control-Expose-Headers", corsExposeHeaders);
      headers.set("Cross-Origin-Resource-Policy", "cross-origin");
      if (isOptions && corsMaxAge != null) {
        headers.set("Access-Control-Max-Age", String(corsMaxAge));
      }
    }
  }
  function resolveRoute(path) {
    if (!path.startsWith(`${prefix}/`))
      return null;
    const subPath = path.slice(prefix.length + 1);
    if (subPath.endsWith("/init"))
      return { methodName: subPath.slice(0, -5), action: "init" };
    if (subPath.endsWith("/exchange"))
      return { methodName: subPath.slice(0, -9), action: "exchange" };
    return { methodName: subPath, action: "call" };
  }
  function negotiateResponseEncoding(request) {
    return pickResponseEncoding(request.headers.get("Accept-Encoding"), request.headers.get(VGI_ACCEPT_ENCODING_HEADER), canProduceEncoding);
  }
  async function compressIfAccepted(response, negotiated) {
    if (compressionLevel == null)
      return response;
    const { codec, usedCustom } = negotiated;
    if (!codec)
      return response;
    const responseBody = new Uint8Array(await response.arrayBuffer());
    const compressed = codec === "zstd" ? await zstdCompress(responseBody, compressionLevel) : await gzipCompress(responseBody);
    const headers = new Headers(response.headers);
    const useCustomHeader = usedCustom || stampCustomContentEncoding;
    headers.set(useCustomHeader ? VGI_CONTENT_ENCODING_HEADER : CONTENT_ENCODING_HEADER, codec);
    return new Response(compressed, {
      status: response.status,
      headers
    });
  }
  function unauthorizedResponse(reason, detail, headers) {
    headers.set("Content-Type", "application/json");
    headers.set(AUTH_REASON_HEADER, reason);
    if (proxyHint)
      headers.set(AUTH_PROXY_REQUIRED_HEADER, "true");
    headers.set("Cache-Control", "no-store");
    return new Response(unauthorizedEnvelope(reason, detail, proxyHint), { status: 401, headers });
  }
  function makeErrorResponse(error, statusCode, schema2 = EMPTY_SCHEMA2) {
    const errBatch = buildErrorBatch(schema2, error, serverId, null);
    const body = serializeIpcStream(schema2, [errBatch]);
    const resp = arrowResponse(body, statusCode);
    addCorsHeaders(resp.headers);
    return resp;
  }
  const enableHealthEndpoint = options?.enableHealthEndpoint ?? true;
  const healthPath = `${prefix}/health`;
  const healthBody = enableHealthEndpoint ? JSON.stringify({ status: "ok", server_id: serverId, protocol: displayName }) : null;
  const dispatchRequest = async function handler(request, deferral, egress, requestId = null) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (pkceConfig && path === `${prefix}/_oauth/token` && (request.method === "POST" || request.method === "OPTIONS")) {
      return handleOAuthTokenProxy(request, pkceConfig);
    }
    if (healthBody !== null && (request.method === "GET" || request.method === "HEAD") && path === healthPath) {
      const headers = new Headers({ "Content-Type": "application/json" });
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      if (request.method === "HEAD") {
        headers.set("Content-Length", String(new TextEncoder().encode(healthBody).byteLength));
        return new Response(null, { status: 200, headers });
      }
      return new Response(healthBody, { status: 200, headers });
    }
    if (oauthMetadata && path === wellKnownPath(prefix)) {
      if (request.method !== "GET") {
        return new Response("Method Not Allowed", { status: 405 });
      }
      const metaJson = oauthResourceMetadataToJson(oauthMetadata);
      if (pkceConfig && oauthMetadata.clientSecret) {
        const resourceUrl = new URL(oauthMetadata.resource);
        metaJson.token_endpoint = `${resourceUrl.protocol}//${resourceUrl.host}${prefix}/_oauth/token`;
      }
      const body2 = JSON.stringify(metaJson);
      const headers = new Headers({
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=60"
      });
      addCorsHeaders(headers);
      return new Response(body2, { status: 200, headers });
    }
    if (request.method === "OPTIONS") {
      const headers = new Headers;
      addCorsHeaders(headers, true, request.headers.get("Access-Control-Request-Headers"));
      addCapabilityHeaders(headers, true);
      return new Response(null, { status: 204, headers });
    }
    if (request.method === "GET") {
      if (pkceConfig) {
        if (path === `${prefix}/_oauth/callback`) {
          return handleOAuthCallback(request, pkceConfig);
        }
        if (path === `${prefix}/_oauth/logout`) {
          return handleOAuthLogout(request, pkceConfig);
        }
        const earlyRedirect = handleEarlyReturnTo(request, pkceConfig);
        if (earlyRedirect)
          return earlyRedirect;
      }
      if (authenticate && pkceConfig) {
        try {
          await authenticate(request);
        } catch {
          const redirect = await handleBrowserGetRedirect(request, pkceConfig);
          if (redirect)
            return redirect;
        }
      }
      if (extraRoutes) {
        const contributed = await extraRoutes(request, {
          url,
          prefix,
          serverId,
          oauthActive,
          addCorsHeaders
        });
        if (contributed)
          return contributed;
      }
      if (genericLandingHtml && (path === prefix || path === `${prefix}/`)) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(genericLandingHtml, { status: 200, headers });
      }
      if (describeHtml && path === `${prefix}/describe`) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(describeHtml, { status: 200, headers });
      }
      if (notFoundHtml) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(notFoundHtml, { status: 404, headers });
      }
      return new Response("Not Found", { status: 404 });
    }
    if (request.method === "DELETE" && stickyEnabled && sessionRegistry && path === `${prefix}/${SESSION_ENDPOINT}`) {
      const headers = new Headers;
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      const tokenHeader = (request.headers.get(SESSION_HEADER) ?? "").trim();
      if (!tokenHeader) {
        return new Response(null, { status: 200, headers });
      }
      let principalKey = sessionPrincipalKey(false, null, null);
      let aadPrincipal = null;
      if (authenticate) {
        try {
          const auth2 = await authenticate(request);
          if (auth2?.authenticated) {
            aadPrincipal = auth2.principal ?? "";
            principalKey = sessionPrincipalKey(true, auth2.domain, auth2.principal);
          }
        } catch {}
      }
      const aad = computeAad(aadPrincipal);
      let opened;
      try {
        opened = openSessionToken(tokenHeader, tokenKey, aad);
      } catch {
        return new Response(null, { status: 200, headers });
      }
      if (opened.serverId !== serverId) {
        return new Response(null, { status: 200, headers });
      }
      const entry = sessionRegistry.get(opened.sessionId, principalKey);
      if (!entry) {
        return new Response(null, { status: 200, headers });
      }
      const release = await entry.lock.acquire();
      try {
        sessionRegistry.close(opened.sessionId);
      } finally {
        release();
      }
      headers.set(SESSION_CLOSE_HEADER, "true");
      return new Response(null, { status: 204, headers });
    }
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }
    if (!introspector && path === introspectPath) {
      const response = introspectionDisabledResponse();
      addCorsHeaders(response.headers);
      return response;
    }
    const streamObserver = {};
    const ctx = { ...baseCtx, cookies: parseRequestCookies(request), egress, streamObserver };
    if (authenticate) {
      try {
        ctx.authContext = await authenticate(request);
      } catch (error) {
        if (error instanceof AuthUnavailableError) {
          const headers2 = new Headers({ "Content-Type": "application/json", "Cache-Control": "no-store" });
          addCorsHeaders(headers2);
          headers2.set("Retry-After", String(error.retryAfter));
          return new Response(JSON.stringify({ error: "authentication_unavailable", detail: error.detail }), {
            status: 503,
            headers: headers2
          });
        }
        const headers = new Headers;
        addCorsHeaders(headers);
        if (oauthMetadata) {
          const metadataUrl = new URL(request.url);
          metadataUrl.pathname = wellKnownPath(prefix);
          metadataUrl.search = "";
          headers.set("WWW-Authenticate", buildWwwAuthenticateHeader(metadataUrl.toString(), oauthMetadata.clientId, oauthMetadata.clientSecret, oauthMetadata.useIdTokenAsBearer, oauthMetadata.deviceCodeClientId, oauthMetadata.deviceCodeClientSecret));
        }
        const { reason, detail } = classifyAuthFailure(error);
        return unauthorizedResponse(reason, detail, headers);
      }
    }
    if (introspector && path === introspectPath) {
      const response = await introspector.handle(request, ctx.authContext);
      addCorsHeaders(response.headers);
      addCapabilityHeaders(response.headers);
      return response;
    }
    const responseEncoding = negotiateResponseEncoding(request);
    let stickyLockRelease = null;
    let stickySink = null;
    if (stickyEnabled && sessionRegistry) {
      const auth2 = ctx.authContext;
      const aadPrincipal = auth2?.authenticated ? auth2.principal ?? "" : null;
      const principalKey = sessionPrincipalKey(!!auth2?.authenticated, auth2?.domain, auth2?.principal);
      const aad = computeAad(aadPrincipal);
      const acceptOpens = (request.headers.get(SESSION_ACCEPT_HEADER) ?? "").trim().toLowerCase() === "true";
      const sessionHeader = (request.headers.get(SESSION_HEADER) ?? "").trim();
      let resumeState = null;
      let resumeSessionId = null;
      if (sessionHeader) {
        let opened;
        try {
          opened = openSessionToken(sessionHeader, tokenKey, aad);
          if (opened.serverId !== serverId) {
            throw new SessionLostError("session token was issued by a different worker (server_id mismatch)");
          }
        } catch (err2) {
          const e = err2 instanceof Error ? err2 : new Error(String(err2));
          const r = makeErrorResponse(e, 500);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, responseEncoding);
        }
        const entry = sessionRegistry.get(opened.sessionId, principalKey);
        if (!entry) {
          const r = makeErrorResponse(new SessionLostError("session not found, expired, or principal mismatch"), 500);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, responseEncoding);
        }
        stickyLockRelease = await entry.lock.acquire();
        resumeState = entry.state;
        resumeSessionId = sessionIdHex(opened.sessionId);
      }
      const sink = {
        acceptOpens,
        state: resumeState,
        sessionId: resumeSessionId,
        mintToken: null,
        closed: false,
        action: sessionHeader ? "resume" : "none",
        _open(state, ttl) {
          const { sessionId, expiresAt } = sessionRegistry.open(state, ttl, principalKey);
          sink.sessionId = sessionIdHex(sessionId);
          sink.state = state;
          sink.mintToken = sealSessionToken(serverId, sessionId, expiresAt, tokenKey, aad);
        },
        _close() {
          if (sink.closed)
            return;
          const sid = sink.sessionId;
          if (!sid)
            return;
          const bytes = new Uint8Array(sid.length / 2);
          for (let i = 0;i < bytes.length; i++)
            bytes[i] = parseInt(sid.slice(i * 2, i * 2 + 2), 16);
          if (stickyLockRelease) {
            stickyLockRelease();
            stickyLockRelease = null;
          }
          sessionRegistry.close(bytes);
          sink.state = null;
          sink.closed = true;
        }
      };
      stickySink = sink;
      ctx.stickyContext = sink;
    }
    const specialPost = path === `${prefix}/${UPLOAD_URL_METHOD}/init` || path === `${prefix}/${DESCRIBE_METHOD_NAME}`;
    const route = specialPost ? null : resolveRoute(path);
    if (!specialPost) {
      if (!route) {
        if (stickyLockRelease)
          stickyLockRelease();
        return new Response("Not Found", { status: 404 });
      }
      if (!methods.has(route.methodName)) {
        if (stickyLockRelease)
          stickyLockRelease();
        const available = [...methods.keys()].sort();
        const err2 = new MethodNotImplementedError(`Unknown method: '${route.methodName}'. Available methods: [${available.join(", ")}]`);
        return compressIfAccepted(makeErrorResponse(err2, 404), responseEncoding);
      }
    }
    const contentType = request.headers.get("Content-Type");
    if (!contentType || !contentType.includes(ARROW_CONTENT_TYPE)) {
      if (stickyLockRelease)
        stickyLockRelease();
      return new Response(`Unsupported Media Type: expected ${ARROW_CONTENT_TYPE}`, { status: 415 });
    }
    const exemptFromMaxBytes = path === healthPath || path === `${prefix}/${UPLOAD_URL_METHOD}/init` || path === `${prefix}/__capabilities__`;
    if (maxRequestBytes != null && !exemptFromMaxBytes) {
      const contentLength = request.headers.get("Content-Length");
      if (contentLength && parseInt(contentLength, 10) > maxRequestBytes) {
        return new Response("Request body too large", { status: 413 });
      }
    }
    let body = new Uint8Array(await request.arrayBuffer());
    const requestWireBytes = body.byteLength;
    if (maxRequestBytes != null && !exemptFromMaxBytes && body.byteLength > maxRequestBytes) {
      return new Response("Request body too large", { status: 413 });
    }
    const contentEncoding = (request.headers.get("Content-Encoding") ?? "").trim().toLowerCase();
    if (contentEncoding === "zstd" || contentEncoding === "gzip") {
      try {
        body = contentEncoding === "zstd" ? await zstdDecompress(body, maxDecompressedRequestBytes) : await gzipDecompress(body, maxDecompressedRequestBytes);
      } catch (error) {
        const message = error?.message ?? `${contentEncoding} decompression failed`;
        const status = message.includes("exceed") || message.includes("cap") ? 413 : 400;
        const headers = new Headers({ "Content-Type": "text/plain" });
        addCorsHeaders(headers);
        addCapabilityHeaders(headers);
        return new Response(message, { status, headers });
      }
    } else if (contentEncoding) {
      const headers = new Headers({ "Content-Type": "text/plain" });
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      return new Response(`Unsupported Content-Encoding: ${contentEncoding}`, { status: 415, headers });
    }
    if (path === `${prefix}/${UPLOAD_URL_METHOD}/init`) {
      if (!uploadUrlProvider) {
        return new Response("Not Found", { status: 404 });
      }
      try {
        const { schema: reqSchema, batch: reqBatch } = await readRequestFromBody(body);
        const parsed = parseRequest(reqSchema, reqBatch);
        if (parsed.methodName !== UPLOAD_URL_METHOD) {
          throw new HttpRpcError(`Method name in request '${parsed.methodName}' does not match URL '${UPLOAD_URL_METHOD}'`, 400);
        }
        const rawCount = parsed.params.count;
        let count = typeof rawCount === "bigint" ? Number(rawCount) : Number(rawCount ?? 1);
        if (!Number.isFinite(count) || count < 1)
          count = 1;
        if (count > MAX_UPLOAD_URL_COUNT)
          count = MAX_UPLOAD_URL_COUNT;
        const urls = [];
        for (let i = 0;i < count; i++) {
          urls.push(await uploadUrlProvider.generateUploadUrl());
        }
        const expiresAt = urls.map((u) => u.expiresAt.getTime());
        const resultBatch = batchFromColumns(UPLOAD_URL_RESPONSE_SCHEMA, {
          upload_url: urls.map((u) => u.uploadUrl),
          download_url: urls.map((u) => u.downloadUrl),
          expires_at: expiresAt
        });
        const responseBody = serializeIpcStream(UPLOAD_URL_RESPONSE_SCHEMA, [resultBatch]);
        const response = arrowResponse(responseBody);
        addCorsHeaders(response.headers);
        addCapabilityHeaders(response.headers);
        return compressIfAccepted(response, responseEncoding);
      } catch (error) {
        if (error instanceof HttpRpcError) {
          const r2 = makeErrorResponse(error, error.statusCode, UPLOAD_URL_RESPONSE_SCHEMA);
          addCapabilityHeaders(r2.headers);
          return compressIfAccepted(r2, responseEncoding);
        }
        const r = makeErrorResponse(error, 500, UPLOAD_URL_RESPONSE_SCHEMA);
        addCapabilityHeaders(r.headers);
        return compressIfAccepted(r, responseEncoding);
      }
    }
    if (path === `${prefix}/${DESCRIBE_METHOD_NAME}`) {
      try {
        const response = await httpDispatchDescribe(protocol.name, methods, serverId, protocol.protocolVersion || undefined);
        addCorsHeaders(response.headers);
        return compressIfAccepted(response, responseEncoding);
      } catch (error) {
        return compressIfAccepted(makeErrorResponse(error, 500), responseEncoding);
      }
    }
    const { methodName, action } = route;
    const method = methods.get(methodName);
    if (protocol.protocolVersionParts !== null && methodName !== DESCRIBE_METHOD_NAME && action !== "exchange") {
      try {
        let reqMeta;
        try {
          const peeked = deserializeBatch(body);
          reqMeta = peeked.metadata ?? undefined;
        } catch {}
        enforceProtocolVersion(reqMeta);
      } catch (exc) {
        const errSchema = method.type === "unary" /* UNARY */ ? method.resultSchema : EMPTY_SCHEMA2;
        const errBatch = buildErrorBatch(errSchema, exc, serverId, null);
        const errBody = serializeIpcStream(errSchema, [errBatch]);
        const response = arrowResponse(errBody, 400);
        addCorsHeaders(response.headers);
        addCapabilityHeaders(response.headers);
        return compressIfAccepted(response, responseEncoding);
      }
    }
    await notifyTransport(transportKind);
    const methodType = method.type === "unary" /* UNARY */ ? "unary" : "stream";
    const protocolHash = await getProtocolHash();
    const auth = ctx.authContext;
    const info = {
      method: methodName,
      methodType,
      serverId,
      requestId,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: transportKind,
      principal: auth?.principal ?? "",
      authDomain: auth?.domain ?? "",
      authenticated: auth?.authenticated ?? false,
      requestData: action === "call" || action === "init" ? body : undefined,
      claims: auth?.claims,
      requestBytes: requestWireBytes,
      deferral
    };
    const stats = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0
    };
    const hookToken = dispatchHook?.onDispatchStart(info);
    let dispatchError;
    try {
      let response;
      if (action === "call") {
        if (method.type !== "unary" /* UNARY */) {
          throw new HttpRpcError(`Method '${methodName}' is a stream method. Use /init and /exchange endpoints.`, 400);
        }
        response = await httpDispatchUnary(method, body, ctx);
      } else if (action === "init") {
        if (method.type !== "stream" /* STREAM */) {
          throw new HttpRpcError(`Method '${methodName}' is a unary method. Use POST ${prefix}/${methodName} instead.`, 400);
        }
        response = await httpDispatchStreamInit(method, body, ctx);
      } else {
        if (method.type !== "stream" /* STREAM */) {
          throw new HttpRpcError(`Method '${methodName}' is a unary method. Use POST ${prefix}/${methodName} instead.`, 400);
        }
        response = await httpDispatchStreamExchange(method, body, ctx);
      }
      const internalError = response.__dispatchError;
      if (internalError) {
        dispatchError = internalError instanceof Error ? internalError : new Error(String(internalError));
      }
      addCorsHeaders(response.headers);
      addCapabilityHeaders(response.headers);
      applyStickyResponseHeaders(response.headers, stickySink);
      info.httpStatus = response.status;
      return compressIfAccepted(response, responseEncoding);
    } catch (error) {
      dispatchError = error instanceof Error ? error : new Error(String(error));
      if (error instanceof HttpRpcError) {
        const r2 = makeErrorResponse(error, error.statusCode);
        addCapabilityHeaders(r2.headers);
        applyStickyResponseHeaders(r2.headers, stickySink);
        info.httpStatus = r2.status;
        return compressIfAccepted(r2, responseEncoding);
      }
      const r = makeErrorResponse(error, 500);
      addCapabilityHeaders(r.headers);
      applyStickyResponseHeaders(r.headers, stickySink);
      info.httpStatus = r.status;
      return compressIfAccepted(r, responseEncoding);
    } finally {
      if (stickySink) {
        if (stickySink.sessionId)
          info.sessionId = stickySink.sessionId;
        info.sessionAction = stickySink.action;
      }
      if (streamObserver.streamId)
        info.streamId = streamObserver.streamId;
      if (streamObserver.cancelled)
        info.cancelled = true;
      if (egress?.externalizedBytes)
        info.externalizedBytes = egress.externalizedBytes;
      dispatchHook?.onDispatchEnd(hookToken, info, stats, dispatchError);
      if (stickyLockRelease) {
        try {
          stickyLockRelease();
        } catch {}
        stickyLockRelease = null;
      }
    }
  };
  return async function handler(request) {
    const requestId = resolveRequestId(request);
    if (!dispatchHook) {
      const plain = await dispatchRequest(request, undefined, undefined, requestId);
      stampRequestId(plain, requestId);
      return plain;
    }
    const pending = [];
    const deferral = { defer: (emit) => pending.push(emit) };
    const egress = { externalizedBytes: 0 };
    let response = await dispatchRequest(request, deferral, egress, requestId);
    if (pending.length === 0) {
      stampRequestId(response, requestId);
      return response;
    }
    let responseBytes;
    try {
      const measured = await finalBodyBytes(response);
      responseBytes = measured.size;
      response = measured.response;
    } catch {}
    for (const emit of pending)
      emit(responseBytes);
    stampRequestId(response, requestId);
    return response;
  };
  function resolveRequestId(request) {
    const inbound = request.headers.get(REQUEST_ID_HEADER);
    const trimmed = inbound?.trim();
    if (trimmed)
      return trimmed;
    return crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  }
  function stampRequestId(response, requestId) {
    try {
      response.headers.set(REQUEST_ID_HEADER, requestId);
    } catch {}
  }
  async function finalBodyBytes(response) {
    const declared = response.headers.get("Content-Length");
    if (declared !== null) {
      const n = Number(declared);
      if (Number.isFinite(n) && n >= 0)
        return { size: n, response };
    }
    if (response.body === null)
      return { size: 0, response };
    const body = new Uint8Array(await response.arrayBuffer());
    return {
      size: body.byteLength,
      response: new Response(body, { status: response.status, headers: response.headers })
    };
  }
  function applyStickyResponseHeaders(headers, sink) {
    if (!sink)
      return;
    if (sink.mintToken !== null) {
      headers.set(SESSION_HEADER, sink.mintToken);
      for (const [name, value] of stickyEchoHeadersArr) {
        headers.set(`${ECHO_HEADER_PREFIX}${name}`, value);
      }
    }
    if (sink.closed) {
      headers.set(SESSION_CLOSE_HEADER, "true");
    }
  }
}
// node_modules/oauth4webapi/build/index.js
var USER_AGENT;
if (typeof navigator === "undefined" || !navigator.userAgent?.startsWith?.("Mozilla/5.0 ")) {
  const NAME = "oauth4webapi";
  const VERSION = "v3.8.6";
  USER_AGENT = `${NAME}/${VERSION}`;
}
function looseInstanceOf(input, expected) {
  if (input == null) {
    return false;
  }
  try {
    return input instanceof expected || Object.getPrototypeOf(input)[Symbol.toStringTag] === expected.prototype[Symbol.toStringTag];
  } catch {
    return false;
  }
}
var ERR_INVALID_ARG_VALUE = "ERR_INVALID_ARG_VALUE";
var ERR_INVALID_ARG_TYPE = "ERR_INVALID_ARG_TYPE";
function CodedTypeError(message, code, cause) {
  const err2 = new TypeError(message, { cause });
  Object.assign(err2, { code });
  return err2;
}
var allowInsecureRequests = Symbol();
var clockSkew = Symbol();
var clockTolerance = Symbol();
var customFetch = Symbol();
var modifyAssertion = Symbol();
var jweDecrypt = Symbol();
var jwksCache = Symbol();
var encoder = new TextEncoder;
var decoder = new TextDecoder;
function buf(input) {
  if (typeof input === "string") {
    return encoder.encode(input);
  }
  return decoder.decode(input);
}
var encodeBase64Url;
if (Uint8Array.prototype.toBase64) {
  encodeBase64Url = (input) => {
    if (input instanceof ArrayBuffer) {
      input = new Uint8Array(input);
    }
    return input.toBase64({ alphabet: "base64url", omitPadding: true });
  };
} else {
  const CHUNK_SIZE = 32768;
  encodeBase64Url = (input) => {
    if (input instanceof ArrayBuffer) {
      input = new Uint8Array(input);
    }
    const arr = [];
    for (let i = 0;i < input.byteLength; i += CHUNK_SIZE) {
      arr.push(String.fromCharCode.apply(null, input.subarray(i, i + CHUNK_SIZE)));
    }
    return btoa(arr.join("")).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
  };
}
var decodeBase64Url;
if (Uint8Array.fromBase64) {
  decodeBase64Url = (input) => {
    try {
      return Uint8Array.fromBase64(input, { alphabet: "base64url" });
    } catch (cause) {
      throw CodedTypeError("The input to be decoded is not correctly encoded.", ERR_INVALID_ARG_VALUE, cause);
    }
  };
} else {
  decodeBase64Url = (input) => {
    try {
      const binary2 = atob(input.replace(/-/g, "+").replace(/_/g, "/").replace(/\s/g, ""));
      const bytes = new Uint8Array(binary2.length);
      for (let i = 0;i < binary2.length; i++) {
        bytes[i] = binary2.charCodeAt(i);
      }
      return bytes;
    } catch (cause) {
      throw CodedTypeError("The input to be decoded is not correctly encoded.", ERR_INVALID_ARG_VALUE, cause);
    }
  };
}
function b64u(input) {
  if (typeof input === "string") {
    return decodeBase64Url(input);
  }
  return encodeBase64Url(input);
}

class UnsupportedOperationError extends Error {
  code;
  constructor(message, options) {
    super(message, options);
    this.name = this.constructor.name;
    this.code = UNSUPPORTED_OPERATION;
    Error.captureStackTrace?.(this, this.constructor);
  }
}

class OperationProcessingError extends Error {
  code;
  constructor(message, options) {
    super(message, options);
    this.name = this.constructor.name;
    if (options?.code) {
      this.code = options?.code;
    }
    Error.captureStackTrace?.(this, this.constructor);
  }
}
function OPE(message, code, cause) {
  return new OperationProcessingError(message, { code, cause });
}
async function calculateJwkThumbprint(jwk) {
  let components;
  switch (jwk.kty) {
    case "EC":
      components = {
        crv: jwk.crv,
        kty: jwk.kty,
        x: jwk.x,
        y: jwk.y
      };
      break;
    case "OKP":
      components = {
        crv: jwk.crv,
        kty: jwk.kty,
        x: jwk.x
      };
      break;
    case "AKP":
      components = {
        alg: jwk.alg,
        kty: jwk.kty,
        pub: jwk.pub
      };
      break;
    case "RSA":
      components = {
        e: jwk.e,
        kty: jwk.kty,
        n: jwk.n
      };
      break;
    default:
      throw new UnsupportedOperationError("unsupported JWK key type", { cause: jwk });
  }
  return b64u(await crypto.subtle.digest("SHA-256", buf(JSON.stringify(components))));
}
function assertCryptoKey(key, it) {
  if (!(key instanceof CryptoKey)) {
    throw CodedTypeError(`${it} must be a CryptoKey`, ERR_INVALID_ARG_TYPE);
  }
}
function assertPrivateKey(key, it) {
  assertCryptoKey(key, it);
  if (key.type !== "private") {
    throw CodedTypeError(`${it} must be a private CryptoKey`, ERR_INVALID_ARG_VALUE);
  }
}
function assertPublicKey(key, it) {
  assertCryptoKey(key, it);
  if (key.type !== "public") {
    throw CodedTypeError(`${it} must be a public CryptoKey`, ERR_INVALID_ARG_VALUE);
  }
}
function normalizeTyp(value) {
  return value.toLowerCase().replace(/^application\//, "");
}
function isJsonObject(input) {
  if (input === null || typeof input !== "object" || Array.isArray(input)) {
    return false;
  }
  return true;
}
function prepareHeaders(input) {
  if (looseInstanceOf(input, Headers)) {
    input = Object.fromEntries(input.entries());
  }
  const headers = new Headers(input ?? {});
  if (USER_AGENT && !headers.has("user-agent")) {
    headers.set("user-agent", USER_AGENT);
  }
  if (headers.has("authorization")) {
    throw CodedTypeError('"options.headers" must not include the "authorization" header name', ERR_INVALID_ARG_VALUE);
  }
  return headers;
}
function signal(url, value) {
  if (value !== undefined) {
    if (typeof value === "function") {
      value = value(url.href);
    }
    if (!(value instanceof AbortSignal)) {
      throw CodedTypeError('"options.signal" must return or be an instance of AbortSignal', ERR_INVALID_ARG_TYPE);
    }
    return value;
  }
  return;
}
function replaceDoubleSlash(pathname) {
  if (pathname.includes("//")) {
    return pathname.replace("//", "/");
  }
  return pathname;
}
function prependWellKnown(url, wellKnown, allowTerminatingSlash = false) {
  if (url.pathname === "/") {
    url.pathname = wellKnown;
  } else {
    url.pathname = replaceDoubleSlash(`${wellKnown}/${allowTerminatingSlash ? url.pathname : url.pathname.replace(/(\/)$/, "")}`);
  }
  return url;
}
function appendWellKnown(url, wellKnown) {
  url.pathname = replaceDoubleSlash(`${url.pathname}/${wellKnown}`);
  return url;
}
async function performDiscovery(input, urlName, transform, options) {
  if (!(input instanceof URL)) {
    throw CodedTypeError(`"${urlName}" must be an instance of URL`, ERR_INVALID_ARG_TYPE);
  }
  checkProtocol(input, options?.[allowInsecureRequests] !== true);
  const url = transform(new URL(input.href));
  const headers = prepareHeaders(options?.headers);
  headers.set("accept", "application/json");
  return (options?.[customFetch] || fetch)(url.href, {
    body: undefined,
    headers: Object.fromEntries(headers.entries()),
    method: "GET",
    redirect: "manual",
    signal: signal(url, options?.signal)
  });
}
async function discoveryRequest(issuerIdentifier, options) {
  return performDiscovery(issuerIdentifier, "issuerIdentifier", (url) => {
    switch (options?.algorithm) {
      case undefined:
      case "oidc":
        appendWellKnown(url, ".well-known/openid-configuration");
        break;
      case "oauth2":
        prependWellKnown(url, ".well-known/oauth-authorization-server");
        break;
      default:
        throw CodedTypeError('"options.algorithm" must be "oidc" (default), or "oauth2"', ERR_INVALID_ARG_VALUE);
    }
    return url;
  }, options);
}
function assertString(input, it, code, cause) {
  try {
    if (typeof input !== "string") {
      throw CodedTypeError(`${it} must be a string`, ERR_INVALID_ARG_TYPE, cause);
    }
    if (input.length === 0) {
      throw CodedTypeError(`${it} must not be empty`, ERR_INVALID_ARG_VALUE, cause);
    }
  } catch (err2) {
    if (code) {
      throw OPE(err2.message, code, cause);
    }
    throw err2;
  }
}
async function processDiscoveryResponse(expectedIssuerIdentifier, response) {
  const expected = expectedIssuerIdentifier;
  if (!(expected instanceof URL) && expected !== _nodiscoverycheck) {
    throw CodedTypeError('"expectedIssuerIdentifier" must be an instance of URL', ERR_INVALID_ARG_TYPE);
  }
  if (!looseInstanceOf(response, Response)) {
    throw CodedTypeError('"response" must be an instance of Response', ERR_INVALID_ARG_TYPE);
  }
  if (response.status !== 200) {
    throw OPE('"response" is not a conform Authorization Server Metadata response (unexpected HTTP status code)', RESPONSE_IS_NOT_CONFORM, response);
  }
  assertReadableResponse(response);
  const json = await getResponseJsonBody(response);
  assertString(json.issuer, '"response" body "issuer" property', INVALID_RESPONSE, { body: json });
  if (expected !== _nodiscoverycheck && new URL(json.issuer).href !== expected.href) {
    throw OPE('"response" body "issuer" property does not match the expected value', JSON_ATTRIBUTE_COMPARISON, { expected: expected.href, body: json, attribute: "issuer" });
  }
  return json;
}
function assertApplicationJson(response) {
  assertContentType(response, "application/json");
}
function notJson(response, ...types) {
  let msg = '"response" content-type must be ';
  if (types.length > 2) {
    const last = types.pop();
    msg += `${types.join(", ")}, or ${last}`;
  } else if (types.length === 2) {
    msg += `${types[0]} or ${types[1]}`;
  } else {
    msg += types[0];
  }
  return OPE(msg, RESPONSE_IS_NOT_JSON, response);
}
function assertContentTypes(response, ...types) {
  if (!types.includes(getContentType(response))) {
    throw notJson(response, ...types);
  }
}
function assertContentType(response, contentType) {
  if (getContentType(response) !== contentType) {
    throw notJson(response, contentType);
  }
}
function randomBytes4() {
  return b64u(crypto.getRandomValues(new Uint8Array(32)));
}
function psAlg(key) {
  switch (key.algorithm.hash.name) {
    case "SHA-256":
      return "PS256";
    case "SHA-384":
      return "PS384";
    case "SHA-512":
      return "PS512";
    default:
      throw new UnsupportedOperationError("unsupported RsaHashedKeyAlgorithm hash name", {
        cause: key
      });
  }
}
function rsAlg(key) {
  switch (key.algorithm.hash.name) {
    case "SHA-256":
      return "RS256";
    case "SHA-384":
      return "RS384";
    case "SHA-512":
      return "RS512";
    default:
      throw new UnsupportedOperationError("unsupported RsaHashedKeyAlgorithm hash name", {
        cause: key
      });
  }
}
function esAlg(key) {
  switch (key.algorithm.namedCurve) {
    case "P-256":
      return "ES256";
    case "P-384":
      return "ES384";
    case "P-521":
      return "ES512";
    default:
      throw new UnsupportedOperationError("unsupported EcKeyAlgorithm namedCurve", { cause: key });
  }
}
function keyToJws(key) {
  switch (key.algorithm.name) {
    case "RSA-PSS":
      return psAlg(key);
    case "RSASSA-PKCS1-v1_5":
      return rsAlg(key);
    case "ECDSA":
      return esAlg(key);
    case "Ed25519":
    case "ML-DSA-44":
    case "ML-DSA-65":
    case "ML-DSA-87":
      return key.algorithm.name;
    case "EdDSA":
      return "Ed25519";
    default:
      throw new UnsupportedOperationError("unsupported CryptoKey algorithm name", { cause: key });
  }
}
function getClockSkew(client) {
  const skew = client?.[clockSkew];
  return typeof skew === "number" && Number.isFinite(skew) ? skew : 0;
}
function getClockTolerance(client) {
  const tolerance = client?.[clockTolerance];
  return typeof tolerance === "number" && Number.isFinite(tolerance) && Math.sign(tolerance) !== -1 ? tolerance : 30;
}
function epochTime() {
  return Math.floor(Date.now() / 1000);
}
function assertAs(as) {
  if (typeof as !== "object" || as === null) {
    throw CodedTypeError('"as" must be an object', ERR_INVALID_ARG_TYPE);
  }
  assertString(as.issuer, '"as.issuer"');
}
async function signJwt(header, payload, key) {
  if (!key.usages.includes("sign")) {
    throw CodedTypeError('CryptoKey instances used for signing assertions must include "sign" in their "usages"', ERR_INVALID_ARG_VALUE);
  }
  const input = `${b64u(buf(JSON.stringify(header)))}.${b64u(buf(JSON.stringify(payload)))}`;
  const signature = b64u(await crypto.subtle.sign(keyToSubtle(key), key, buf(input)));
  return `${input}.${signature}`;
}
var jwkCache;
async function getSetPublicJwkCache(key, alg) {
  const { kty, e, n, x, y, crv, pub } = await crypto.subtle.exportKey("jwk", key);
  const jwk = { kty, e, n, x, y, crv, pub };
  if (kty === "AKP")
    jwk.alg = alg;
  jwkCache.set(key, jwk);
  return jwk;
}
async function publicJwk(key, alg) {
  jwkCache ||= new WeakMap;
  return jwkCache.get(key) || getSetPublicJwkCache(key, alg);
}
var URLParse = URL.parse ? (url, base) => URL.parse(url, base) : (url, base) => {
  try {
    return new URL(url, base);
  } catch {
    return null;
  }
};
function checkProtocol(url, enforceHttps) {
  if (enforceHttps && url.protocol !== "https:") {
    throw OPE("only requests to HTTPS are allowed", HTTP_REQUEST_FORBIDDEN, url);
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw OPE("only HTTP and HTTPS requests are allowed", REQUEST_PROTOCOL_FORBIDDEN, url);
  }
}
function validateEndpoint(value, endpoint, useMtlsAlias, enforceHttps) {
  let url;
  if (typeof value !== "string" || !(url = URLParse(value))) {
    throw OPE(`authorization server metadata does not contain a valid ${useMtlsAlias ? `"as.mtls_endpoint_aliases.${endpoint}"` : `"as.${endpoint}"`}`, value === undefined ? MISSING_SERVER_METADATA : INVALID_SERVER_METADATA, { attribute: useMtlsAlias ? `mtls_endpoint_aliases.${endpoint}` : endpoint });
  }
  checkProtocol(url, enforceHttps);
  return url;
}
function resolveEndpoint(as, endpoint, useMtlsAlias, enforceHttps) {
  if (useMtlsAlias && as.mtls_endpoint_aliases && endpoint in as.mtls_endpoint_aliases) {
    return validateEndpoint(as.mtls_endpoint_aliases[endpoint], endpoint, useMtlsAlias, enforceHttps);
  }
  return validateEndpoint(as[endpoint], endpoint, useMtlsAlias, enforceHttps);
}
class DPoPHandler {
  #header;
  #privateKey;
  #publicKey;
  #clockSkew;
  #modifyAssertion;
  #map;
  #jkt;
  constructor(client, keyPair, options) {
    assertPrivateKey(keyPair?.privateKey, '"DPoP.privateKey"');
    assertPublicKey(keyPair?.publicKey, '"DPoP.publicKey"');
    if (!keyPair.publicKey.extractable) {
      throw CodedTypeError('"DPoP.publicKey.extractable" must be true', ERR_INVALID_ARG_VALUE);
    }
    this.#modifyAssertion = options?.[modifyAssertion];
    this.#clockSkew = getClockSkew(client);
    this.#privateKey = keyPair.privateKey;
    this.#publicKey = keyPair.publicKey;
    branded.add(this);
  }
  #get(key) {
    this.#map ||= new Map;
    let item = this.#map.get(key);
    if (item) {
      this.#map.delete(key);
      this.#map.set(key, item);
    }
    return item;
  }
  #set(key, val) {
    this.#map ||= new Map;
    this.#map.delete(key);
    if (this.#map.size === 100) {
      this.#map.delete(this.#map.keys().next().value);
    }
    this.#map.set(key, val);
  }
  async calculateThumbprint() {
    if (!this.#jkt) {
      const jwk = await crypto.subtle.exportKey("jwk", this.#publicKey);
      this.#jkt ||= await calculateJwkThumbprint(jwk);
    }
    return this.#jkt;
  }
  async addProof(url, headers, htm, accessToken) {
    const alg = keyToJws(this.#privateKey);
    this.#header ||= {
      alg,
      typ: "dpop+jwt",
      jwk: await publicJwk(this.#publicKey, alg)
    };
    const nonce = this.#get(url.origin);
    const now = epochTime() + this.#clockSkew;
    const payload = {
      iat: now,
      jti: randomBytes4(),
      htm,
      nonce,
      htu: `${url.origin}${url.pathname}`,
      ath: accessToken ? b64u(await crypto.subtle.digest("SHA-256", buf(accessToken))) : undefined
    };
    this.#modifyAssertion?.(this.#header, payload);
    headers.set("dpop", await signJwt(this.#header, payload, this.#privateKey));
  }
  cacheNonce(response, url) {
    try {
      const nonce = response.headers.get("dpop-nonce");
      if (nonce) {
        this.#set(url.origin, nonce);
      }
    } catch {}
  }
}
var tokenMatch = "[a-zA-Z0-9!#$%&\\'\\*\\+\\-\\.\\^_`\\|~]+";
var token68Match = "[a-zA-Z0-9\\-\\._\\~\\+\\/]+={0,2}";
var quotedMatch = '"((?:[^"\\\\]|\\\\[\\s\\S])*)"';
var quotedParamMatcher = "(" + tokenMatch + ")\\s*=\\s*" + quotedMatch;
var paramMatcher = "(" + tokenMatch + ")\\s*=\\s*(" + tokenMatch + ")";
var schemeRE = new RegExp("^[,\\s]*(" + tokenMatch + ")");
var quotedParamRE = new RegExp("^[,\\s]*" + quotedParamMatcher + "[,\\s]*(.*)");
var unquotedParamRE = new RegExp("^[,\\s]*" + paramMatcher + "[,\\s]*(.*)");
var token68ParamRE = new RegExp("^(" + token68Match + ")(?:$|[,\\s])(.*)");
var jwksMap;
function setJwksCache(as, jwks, uat, cache) {
  jwksMap ||= new WeakMap;
  jwksMap.set(as, {
    jwks,
    uat,
    get age() {
      return epochTime() - this.uat;
    }
  });
  if (cache) {
    Object.assign(cache, { jwks: structuredClone(jwks), uat });
  }
}
function isFreshJwksCache(input) {
  if (typeof input !== "object" || input === null) {
    return false;
  }
  if (!("uat" in input) || typeof input.uat !== "number" || epochTime() - input.uat >= 300) {
    return false;
  }
  if (!("jwks" in input) || !isJsonObject(input.jwks) || !Array.isArray(input.jwks.keys) || !Array.prototype.every.call(input.jwks.keys, isJsonObject)) {
    return false;
  }
  return true;
}
function clearJwksCache(as, cache) {
  jwksMap?.delete(as);
  delete cache?.jwks;
  delete cache?.uat;
}
async function getPublicSigKeyFromIssuerJwksUri(as, options, header) {
  const { alg, kid } = header;
  checkSupportedJwsAlg(header);
  if (!jwksMap?.has(as) && isFreshJwksCache(options?.[jwksCache])) {
    setJwksCache(as, options?.[jwksCache].jwks, options?.[jwksCache].uat);
  }
  let jwks;
  let age;
  if (jwksMap?.has(as)) {
    ({ jwks, age } = jwksMap.get(as));
    if (age >= 300) {
      clearJwksCache(as, options?.[jwksCache]);
      return getPublicSigKeyFromIssuerJwksUri(as, options, header);
    }
  } else {
    jwks = await jwksRequest(as, options).then(processJwksResponse);
    age = 0;
    setJwksCache(as, jwks, epochTime(), options?.[jwksCache]);
  }
  let kty;
  switch (alg.slice(0, 2)) {
    case "RS":
    case "PS":
      kty = "RSA";
      break;
    case "ES":
      kty = "EC";
      break;
    case "Ed":
      kty = "OKP";
      break;
    case "ML":
      kty = "AKP";
      break;
    default:
      throw new UnsupportedOperationError("unsupported JWS algorithm", { cause: { alg } });
  }
  const candidates = jwks.keys.filter((jwk2) => {
    if (jwk2.kty !== kty) {
      return false;
    }
    if (kid !== undefined && kid !== jwk2.kid) {
      return false;
    }
    if (jwk2.alg !== undefined && alg !== jwk2.alg) {
      return false;
    }
    if (jwk2.use !== undefined && jwk2.use !== "sig") {
      return false;
    }
    if (jwk2.key_ops?.includes("verify") === false) {
      return false;
    }
    switch (true) {
      case (alg === "ES256" && jwk2.crv !== "P-256"):
      case (alg === "ES384" && jwk2.crv !== "P-384"):
      case (alg === "ES512" && jwk2.crv !== "P-521"):
      case (alg === "Ed25519" && jwk2.crv !== "Ed25519"):
      case (alg === "EdDSA" && jwk2.crv !== "Ed25519"):
        return false;
    }
    return true;
  });
  const { 0: jwk, length } = candidates;
  if (!length) {
    if (age >= 60) {
      clearJwksCache(as, options?.[jwksCache]);
      return getPublicSigKeyFromIssuerJwksUri(as, options, header);
    }
    throw OPE("error when selecting a JWT verification key, no applicable keys found", KEY_SELECTION, { header, candidates, jwks_uri: new URL(as.jwks_uri) });
  }
  if (length !== 1) {
    throw OPE('error when selecting a JWT verification key, multiple applicable keys found, a "kid" JWT Header Parameter is required', KEY_SELECTION, { header, candidates, jwks_uri: new URL(as.jwks_uri) });
  }
  return importJwk(alg, jwk);
}
var skipSubjectCheck = Symbol();
function getContentType(input) {
  return input.headers.get("content-type")?.split(";")[0];
}
var idTokenClaims = new WeakMap;
var jwtRefs = new WeakMap;
function validateAudience(expected, result) {
  if (Array.isArray(result.claims.aud)) {
    if (!result.claims.aud.includes(expected)) {
      throw OPE('unexpected JWT "aud" (audience) claim value', JWT_CLAIM_COMPARISON, {
        expected,
        claims: result.claims,
        claim: "aud"
      });
    }
  } else if (result.claims.aud !== expected) {
    throw OPE('unexpected JWT "aud" (audience) claim value', JWT_CLAIM_COMPARISON, {
      expected,
      claims: result.claims,
      claim: "aud"
    });
  }
  return result;
}
function validateIssuer(as, result) {
  const expected = as[_expectedIssuer]?.(result) ?? as.issuer;
  if (result.claims.iss !== expected) {
    throw OPE('unexpected JWT "iss" (issuer) claim value', JWT_CLAIM_COMPARISON, {
      expected,
      claims: result.claims,
      claim: "iss"
    });
  }
  return result;
}
var branded = new WeakSet;
var nopkce = Symbol();
var jwtClaimNames = {
  aud: "audience",
  c_hash: "code hash",
  client_id: "client id",
  exp: "expiration time",
  iat: "issued at",
  iss: "issuer",
  jti: "jwt id",
  nonce: "nonce",
  s_hash: "state hash",
  sub: "subject",
  ath: "access token hash",
  htm: "http method",
  htu: "http uri",
  cnf: "confirmation",
  auth_time: "authentication time"
};
function validatePresence(required, result) {
  for (const claim of required) {
    if (result.claims[claim] === undefined) {
      throw OPE(`JWT "${claim}" (${jwtClaimNames[claim]}) claim missing`, INVALID_RESPONSE, {
        claims: result.claims
      });
    }
  }
  return result;
}
var expectNoNonce = Symbol();
var skipAuthTimeCheck = Symbol();
var UNSUPPORTED_OPERATION = "OAUTH_UNSUPPORTED_OPERATION";
var PARSE_ERROR = "OAUTH_PARSE_ERROR";
var INVALID_RESPONSE = "OAUTH_INVALID_RESPONSE";
var INVALID_REQUEST = "OAUTH_INVALID_REQUEST";
var RESPONSE_IS_NOT_JSON = "OAUTH_RESPONSE_IS_NOT_JSON";
var RESPONSE_IS_NOT_CONFORM = "OAUTH_RESPONSE_IS_NOT_CONFORM";
var HTTP_REQUEST_FORBIDDEN = "OAUTH_HTTP_REQUEST_FORBIDDEN";
var REQUEST_PROTOCOL_FORBIDDEN = "OAUTH_REQUEST_PROTOCOL_FORBIDDEN";
var JWT_TIMESTAMP_CHECK = "OAUTH_JWT_TIMESTAMP_CHECK_FAILED";
var JWT_CLAIM_COMPARISON = "OAUTH_JWT_CLAIM_COMPARISON_FAILED";
var JSON_ATTRIBUTE_COMPARISON = "OAUTH_JSON_ATTRIBUTE_COMPARISON_FAILED";
var KEY_SELECTION = "OAUTH_KEY_SELECTION_FAILED";
var MISSING_SERVER_METADATA = "OAUTH_MISSING_SERVER_METADATA";
var INVALID_SERVER_METADATA = "OAUTH_INVALID_SERVER_METADATA";
function checkJwtType(expected, result) {
  if (typeof result.header.typ !== "string" || normalizeTyp(result.header.typ) !== expected) {
    throw OPE('unexpected JWT "typ" header parameter value', INVALID_RESPONSE, {
      header: result.header
    });
  }
  return result;
}
function assertReadableResponse(response) {
  if (response.bodyUsed) {
    throw CodedTypeError('"response" body has been used already', ERR_INVALID_ARG_VALUE);
  }
}
async function jwksRequest(as, options) {
  assertAs(as);
  const url = resolveEndpoint(as, "jwks_uri", false, options?.[allowInsecureRequests] !== true);
  const headers = prepareHeaders(options?.headers);
  headers.set("accept", "application/json");
  headers.append("accept", "application/jwk-set+json");
  return (options?.[customFetch] || fetch)(url.href, {
    body: undefined,
    headers: Object.fromEntries(headers.entries()),
    method: "GET",
    redirect: "manual",
    signal: signal(url, options?.signal)
  });
}
async function processJwksResponse(response) {
  if (!looseInstanceOf(response, Response)) {
    throw CodedTypeError('"response" must be an instance of Response', ERR_INVALID_ARG_TYPE);
  }
  if (response.status !== 200) {
    throw OPE('"response" is not a conform JSON Web Key Set response (unexpected HTTP status code)', RESPONSE_IS_NOT_CONFORM, response);
  }
  assertReadableResponse(response);
  const json = await getResponseJsonBody(response, (response2) => assertContentTypes(response2, "application/json", "application/jwk-set+json"));
  if (!Array.isArray(json.keys)) {
    throw OPE('"response" body "keys" property must be an array', INVALID_RESPONSE, { body: json });
  }
  if (!Array.prototype.every.call(json.keys, isJsonObject)) {
    throw OPE('"response" body "keys" property members must be JWK formatted objects', INVALID_RESPONSE, { body: json });
  }
  return json;
}
function supported(alg) {
  switch (alg) {
    case "PS256":
    case "ES256":
    case "RS256":
    case "PS384":
    case "ES384":
    case "RS384":
    case "PS512":
    case "ES512":
    case "RS512":
    case "Ed25519":
    case "EdDSA":
    case "ML-DSA-44":
    case "ML-DSA-65":
    case "ML-DSA-87":
      return true;
    default:
      return false;
  }
}
function checkSupportedJwsAlg(header) {
  if (!supported(header.alg)) {
    throw new UnsupportedOperationError('unsupported JWS "alg" identifier', {
      cause: { alg: header.alg }
    });
  }
}
function checkRsaKeyAlgorithm(key) {
  const { algorithm } = key;
  if (typeof algorithm.modulusLength !== "number" || algorithm.modulusLength < 2048) {
    throw new UnsupportedOperationError(`unsupported ${algorithm.name} modulusLength`, {
      cause: key
    });
  }
}
function ecdsaHashName(key) {
  const { algorithm } = key;
  switch (algorithm.namedCurve) {
    case "P-256":
      return "SHA-256";
    case "P-384":
      return "SHA-384";
    case "P-521":
      return "SHA-512";
    default:
      throw new UnsupportedOperationError("unsupported ECDSA namedCurve", { cause: key });
  }
}
function keyToSubtle(key) {
  switch (key.algorithm.name) {
    case "ECDSA":
      return {
        name: key.algorithm.name,
        hash: ecdsaHashName(key)
      };
    case "RSA-PSS": {
      checkRsaKeyAlgorithm(key);
      switch (key.algorithm.hash.name) {
        case "SHA-256":
        case "SHA-384":
        case "SHA-512":
          return {
            name: key.algorithm.name,
            saltLength: parseInt(key.algorithm.hash.name.slice(-3), 10) >> 3
          };
        default:
          throw new UnsupportedOperationError("unsupported RSA-PSS hash name", { cause: key });
      }
    }
    case "RSASSA-PKCS1-v1_5":
      checkRsaKeyAlgorithm(key);
      return key.algorithm.name;
    case "ML-DSA-44":
    case "ML-DSA-65":
    case "ML-DSA-87":
    case "Ed25519":
      return key.algorithm.name;
  }
  throw new UnsupportedOperationError("unsupported CryptoKey algorithm name", { cause: key });
}
async function validateJwsSignature(protectedHeader, payload, key, signature) {
  const data = buf(`${protectedHeader}.${payload}`);
  const algorithm = keyToSubtle(key);
  const verified = await crypto.subtle.verify(algorithm, key, signature, data);
  if (!verified) {
    throw OPE("JWT signature verification failed", INVALID_RESPONSE, {
      key,
      data,
      signature,
      algorithm
    });
  }
}
async function validateJwt(jws, checkAlg, clockSkew2, clockTolerance2, decryptJwt) {
  let { 0: protectedHeader, 1: payload, length } = jws.split(".");
  if (length === 5) {
    if (decryptJwt !== undefined) {
      jws = await decryptJwt(jws);
      ({ 0: protectedHeader, 1: payload, length } = jws.split("."));
    } else {
      throw new UnsupportedOperationError("JWE decryption is not configured", { cause: jws });
    }
  }
  if (length !== 3) {
    throw OPE("Invalid JWT", INVALID_RESPONSE, jws);
  }
  let header;
  try {
    header = JSON.parse(buf(b64u(protectedHeader)));
  } catch (cause) {
    throw OPE("failed to parse JWT Header body as base64url encoded JSON", PARSE_ERROR, cause);
  }
  if (!isJsonObject(header)) {
    throw OPE("JWT Header must be a top level object", INVALID_RESPONSE, jws);
  }
  checkAlg(header);
  if (header.crit !== undefined) {
    throw new UnsupportedOperationError('no JWT "crit" header parameter extensions are supported', {
      cause: { header }
    });
  }
  let claims;
  try {
    claims = JSON.parse(buf(b64u(payload)));
  } catch (cause) {
    throw OPE("failed to parse JWT Payload body as base64url encoded JSON", PARSE_ERROR, cause);
  }
  if (!isJsonObject(claims)) {
    throw OPE("JWT Payload must be a top level object", INVALID_RESPONSE, jws);
  }
  const now = epochTime() + clockSkew2;
  if (claims.exp !== undefined) {
    if (typeof claims.exp !== "number") {
      throw OPE('unexpected JWT "exp" (expiration time) claim type', INVALID_RESPONSE, { claims });
    }
    if (claims.exp <= now - clockTolerance2) {
      throw OPE('unexpected JWT "exp" (expiration time) claim value, expiration is past current timestamp', JWT_TIMESTAMP_CHECK, { claims, now, tolerance: clockTolerance2, claim: "exp" });
    }
  }
  if (claims.iat !== undefined) {
    if (typeof claims.iat !== "number") {
      throw OPE('unexpected JWT "iat" (issued at) claim type', INVALID_RESPONSE, { claims });
    }
  }
  if (claims.iss !== undefined) {
    if (typeof claims.iss !== "string") {
      throw OPE('unexpected JWT "iss" (issuer) claim type', INVALID_RESPONSE, { claims });
    }
  }
  if (claims.nbf !== undefined) {
    if (typeof claims.nbf !== "number") {
      throw OPE('unexpected JWT "nbf" (not before) claim type', INVALID_RESPONSE, { claims });
    }
    if (claims.nbf > now + clockTolerance2) {
      throw OPE('unexpected JWT "nbf" (not before) claim value', JWT_TIMESTAMP_CHECK, {
        claims,
        now,
        tolerance: clockTolerance2,
        claim: "nbf"
      });
    }
  }
  if (claims.aud !== undefined) {
    if (typeof claims.aud !== "string" && !Array.isArray(claims.aud)) {
      throw OPE('unexpected JWT "aud" (audience) claim type', INVALID_RESPONSE, { claims });
    }
  }
  return { header, claims, jwt: jws };
}
function checkSigningAlgorithm(client, issuer, fallback, header) {
  if (client !== undefined) {
    if (typeof client === "string" ? header.alg !== client : !client.includes(header.alg)) {
      throw OPE('unexpected JWT "alg" header parameter', INVALID_RESPONSE, {
        header,
        expected: client,
        reason: "client configuration"
      });
    }
    return;
  }
  if (Array.isArray(issuer)) {
    if (!issuer.includes(header.alg)) {
      throw OPE('unexpected JWT "alg" header parameter', INVALID_RESPONSE, {
        header,
        expected: issuer,
        reason: "authorization server metadata"
      });
    }
    return;
  }
  if (fallback !== undefined) {
    if (typeof fallback === "string" ? header.alg !== fallback : typeof fallback === "function" ? !fallback(header.alg) : !fallback.includes(header.alg)) {
      throw OPE('unexpected JWT "alg" header parameter', INVALID_RESPONSE, {
        header,
        expected: fallback,
        reason: "default value"
      });
    }
    return;
  }
  throw OPE('missing client or server configuration to verify used JWT "alg" header parameter', undefined, { client, issuer, fallback });
}
var skipStateCheck = Symbol();
var expectNoState = Symbol();
function algToSubtle(alg) {
  switch (alg) {
    case "PS256":
    case "PS384":
    case "PS512":
      return { name: "RSA-PSS", hash: `SHA-${alg.slice(-3)}` };
    case "RS256":
    case "RS384":
    case "RS512":
      return { name: "RSASSA-PKCS1-v1_5", hash: `SHA-${alg.slice(-3)}` };
    case "ES256":
    case "ES384":
      return { name: "ECDSA", namedCurve: `P-${alg.slice(-3)}` };
    case "ES512":
      return { name: "ECDSA", namedCurve: "P-521" };
    case "EdDSA":
      return "Ed25519";
    case "Ed25519":
    case "ML-DSA-44":
    case "ML-DSA-65":
    case "ML-DSA-87":
      return alg;
    default:
      throw new UnsupportedOperationError("unsupported JWS algorithm", { cause: { alg } });
  }
}
async function importJwk(alg, jwk) {
  const { ext, key_ops, use, ...key } = jwk;
  return crypto.subtle.importKey("jwk", key, algToSubtle(alg), true, ["verify"]);
}
function normalizeHtu(htu) {
  const url = new URL(htu);
  url.search = "";
  url.hash = "";
  return url.href;
}
async function validateDPoP(request, accessToken, accessTokenClaims, options) {
  const headerValue = request.headers.get("dpop");
  if (headerValue === null) {
    throw OPE("operation indicated DPoP use but the request has no DPoP HTTP Header", INVALID_REQUEST, { headers: request.headers });
  }
  if (request.headers.get("authorization")?.toLowerCase().startsWith("dpop ") === false) {
    throw OPE(`operation indicated DPoP use but the request's Authorization HTTP Header scheme is not DPoP`, INVALID_REQUEST, { headers: request.headers });
  }
  if (typeof accessTokenClaims.cnf?.jkt !== "string") {
    throw OPE("operation indicated DPoP use but the JWT Access Token has no jkt confirmation claim", INVALID_REQUEST, { claims: accessTokenClaims });
  }
  const clockSkew2 = getClockSkew(options);
  const proof = await validateJwt(headerValue, checkSigningAlgorithm.bind(undefined, options?.signingAlgorithms, undefined, supported), clockSkew2, getClockTolerance(options), undefined).then(checkJwtType.bind(undefined, "dpop+jwt")).then(validatePresence.bind(undefined, ["iat", "jti", "ath", "htm", "htu"]));
  const now = epochTime() + clockSkew2;
  const diff = Math.abs(now - proof.claims.iat);
  if (diff > 300) {
    throw OPE("DPoP Proof iat is not recent enough", JWT_TIMESTAMP_CHECK, {
      now,
      claims: proof.claims,
      claim: "iat"
    });
  }
  if (proof.claims.htm !== request.method) {
    throw OPE("DPoP Proof htm mismatch", JWT_CLAIM_COMPARISON, {
      expected: request.method,
      claims: proof.claims,
      claim: "htm"
    });
  }
  if (typeof proof.claims.htu !== "string" || normalizeHtu(proof.claims.htu) !== normalizeHtu(request.url)) {
    throw OPE("DPoP Proof htu mismatch", JWT_CLAIM_COMPARISON, {
      expected: normalizeHtu(request.url),
      claims: proof.claims,
      claim: "htu"
    });
  }
  {
    const expected = b64u(await crypto.subtle.digest("SHA-256", buf(accessToken)));
    if (proof.claims.ath !== expected) {
      throw OPE("DPoP Proof ath mismatch", JWT_CLAIM_COMPARISON, {
        expected,
        claims: proof.claims,
        claim: "ath"
      });
    }
  }
  const { jwk, alg } = proof.header;
  if (!isJsonObject(jwk)) {
    throw OPE("DPoP Proof jwk header parameter must be a JSON object", INVALID_REQUEST, {
      header: proof.header
    });
  }
  {
    const expected = await calculateJwkThumbprint(jwk);
    if (accessTokenClaims.cnf.jkt !== expected) {
      throw OPE("JWT Access Token confirmation mismatch", JWT_CLAIM_COMPARISON, {
        expected,
        claims: accessTokenClaims,
        claim: "cnf.jkt"
      });
    }
  }
  const { 0: protectedHeader, 1: payload, 2: encodedSignature } = headerValue.split(".");
  const signature = b64u(encodedSignature);
  const key = await importJwk(alg, jwk);
  if (key.type !== "public") {
    throw OPE("DPoP Proof jwk header parameter must contain a public key", INVALID_REQUEST, {
      header: proof.header
    });
  }
  await validateJwsSignature(protectedHeader, payload, key, signature);
}
async function validateJwtAccessToken(as, request, expectedAudience, options) {
  assertAs(as);
  if (!looseInstanceOf(request, Request)) {
    throw CodedTypeError('"request" must be an instance of Request', ERR_INVALID_ARG_TYPE);
  }
  assertString(expectedAudience, '"expectedAudience"');
  const authorization = request.headers.get("authorization");
  if (authorization === null) {
    throw OPE('"request" is missing an Authorization HTTP Header', INVALID_REQUEST, {
      headers: request.headers
    });
  }
  let { 0: scheme, 1: accessToken, length } = authorization.split(" ");
  scheme = scheme.toLowerCase();
  switch (scheme) {
    case "dpop":
    case "bearer":
      break;
    default:
      throw new UnsupportedOperationError("unsupported Authorization HTTP Header scheme", {
        cause: { headers: request.headers }
      });
  }
  if (length !== 2) {
    throw OPE("invalid Authorization HTTP Header format", INVALID_REQUEST, {
      headers: request.headers
    });
  }
  const requiredClaims = [
    "iss",
    "exp",
    "aud",
    "sub",
    "iat",
    "jti",
    "client_id"
  ];
  if (options?.requireDPoP || scheme === "dpop" || request.headers.has("dpop")) {
    requiredClaims.push("cnf");
  }
  const { claims, header } = await validateJwt(accessToken, checkSigningAlgorithm.bind(undefined, options?.signingAlgorithms, undefined, supported), getClockSkew(options), getClockTolerance(options), undefined).then(checkJwtType.bind(undefined, "at+jwt")).then(validatePresence.bind(undefined, requiredClaims)).then(validateIssuer.bind(undefined, as)).then(validateAudience.bind(undefined, expectedAudience)).catch(reassignRSCode);
  for (const claim of ["client_id", "jti", "sub"]) {
    if (typeof claims[claim] !== "string") {
      throw OPE(`unexpected JWT "${claim}" claim type`, INVALID_REQUEST, { claims });
    }
  }
  if ("cnf" in claims) {
    if (!isJsonObject(claims.cnf)) {
      throw OPE('unexpected JWT "cnf" (confirmation) claim value', INVALID_REQUEST, { claims });
    }
    const { 0: cnf, length: length2 } = Object.keys(claims.cnf);
    if (length2) {
      if (length2 !== 1) {
        throw new UnsupportedOperationError("multiple confirmation claims are not supported", {
          cause: { claims }
        });
      }
      if (cnf !== "jkt") {
        throw new UnsupportedOperationError("unsupported JWT Confirmation method", {
          cause: { claims }
        });
      }
    }
  }
  const { 0: protectedHeader, 1: payload, 2: encodedSignature } = accessToken.split(".");
  const signature = b64u(encodedSignature);
  const key = await getPublicSigKeyFromIssuerJwksUri(as, options, header);
  await validateJwsSignature(protectedHeader, payload, key, signature);
  if (options?.requireDPoP || scheme === "dpop" || claims.cnf?.jkt !== undefined || request.headers.has("dpop")) {
    await validateDPoP(request, accessToken, claims, options).catch(reassignRSCode);
  }
  return claims;
}
function reassignRSCode(err2) {
  if (err2 instanceof OperationProcessingError && err2?.code === INVALID_REQUEST) {
    err2.code = INVALID_RESPONSE;
  }
  throw err2;
}
async function getResponseJsonBody(response, check = assertApplicationJson) {
  let json;
  try {
    json = await response.json();
  } catch (cause) {
    check(response);
    throw OPE('failed to parse "response" body as JSON', PARSE_ERROR, cause);
  }
  if (!isJsonObject(json)) {
    throw OPE('"response" body must be a top level object', INVALID_RESPONSE, { body: json });
  }
  return json;
}
var _nodiscoverycheck = Symbol();
var _expectedIssuer = Symbol();

// src/http/jwt.ts
function jwtAuthenticate(options) {
  const principalClaim = options.principalClaim ?? "sub";
  const domain = options.domain ?? "jwt";
  const audience = options.audience;
  let asPromise = null;
  async function getAuthorizationServer() {
    if (options.jwksUri) {
      return {
        issuer: options.issuer,
        jwks_uri: options.jwksUri
      };
    }
    const issuerUrl = new URL(options.issuer);
    const response = await discoveryRequest(issuerUrl);
    return processDiscoveryResponse(issuerUrl, response);
  }
  return async function authenticate(request) {
    if (!asPromise) {
      asPromise = getAuthorizationServer();
    }
    let as;
    try {
      as = await asPromise;
    } catch (error) {
      asPromise = null;
      throw error;
    }
    const audiences = Array.isArray(audience) ? audience : [audience];
    let claims;
    let lastError;
    for (const aud of audiences) {
      try {
        claims = await validateJwtAccessToken(as, request, aud);
        break;
      } catch (error) {
        lastError = error;
      }
    }
    if (!claims) {
      throw lastError;
    }
    const principal = claims[principalClaim] ?? null;
    return new AuthContext(domain, true, principal, claims);
  };
}
// src/http/mtls.ts
var _NODE_CRYPTO_MOD2 = "node:crypto";
function _loadNodeCrypto() {
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req) {
    throw new Error("mTLS PEM-based authentication requires Node.js or Bun (node:crypto).");
  }
  const nc = req(_NODE_CRYPTO_MOD2);
  return { X509Certificate: nc.X509Certificate, createHash: nc.createHash };
}
function splitRespectingQuotes(text, delimiter) {
  const parts = [];
  let current = [];
  let inQuotes = false;
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      current.push(ch);
    } else if (ch === "\\" && inQuotes && i + 1 < text.length) {
      current.push(ch);
      current.push(text[i + 1]);
      i++;
    } else if (ch === delimiter && !inQuotes) {
      parts.push(current.join(""));
      current = [];
    } else {
      current.push(ch);
    }
    i++;
  }
  parts.push(current.join(""));
  return parts;
}
function unescapeQuoted(text) {
  return text.replace(/\\(.)/g, "$1");
}
function extractCn(subject) {
  for (const part of subject.split(/(?<!\\),/)) {
    const trimmed = part.trim();
    if (trimmed.toUpperCase().startsWith("CN=")) {
      return trimmed.slice(3);
    }
  }
  return "";
}
function parseXfcc(headerValue) {
  const elements = [];
  for (const rawElement of splitRespectingQuotes(headerValue, ",")) {
    const trimmed = rawElement.trim();
    if (!trimmed)
      continue;
    const pairs = splitRespectingQuotes(trimmed, ";");
    const fields = {};
    for (const pair of pairs) {
      const p = pair.trim();
      if (!p)
        continue;
      const eqIdx = p.indexOf("=");
      if (eqIdx < 0)
        continue;
      const key = p.slice(0, eqIdx).trim().toLowerCase();
      let value = p.slice(eqIdx + 1).trim();
      if (value.length >= 2 && value[0] === '"' && value[value.length - 1] === '"') {
        value = unescapeQuoted(value.slice(1, -1));
      }
      if (key === "cert" || key === "uri" || key === "by") {
        value = decodeURIComponent(value);
      }
      if (key === "dns") {
        const existing = fields.dns;
        if (Array.isArray(existing)) {
          existing.push(value);
        } else {
          fields.dns = [value];
        }
      } else {
        fields[key] = value;
      }
    }
    const dns = Array.isArray(fields.dns) ? fields.dns : [];
    elements.push({
      hash: typeof fields.hash === "string" ? fields.hash : null,
      cert: typeof fields.cert === "string" ? fields.cert : null,
      subject: typeof fields.subject === "string" ? fields.subject : null,
      uri: typeof fields.uri === "string" ? fields.uri : null,
      dns,
      by: typeof fields.by === "string" ? fields.by : null
    });
  }
  return elements;
}
function mtlsAuthenticateXfcc(options) {
  const validate = options?.validate;
  const domain = options?.domain ?? "mtls";
  const selectElement = options?.selectElement ?? "first";
  return async function authenticate(request) {
    const headerValue = request.headers.get("x-forwarded-client-cert");
    if (!headerValue) {
      throw new Error("Missing x-forwarded-client-cert header");
    }
    const elements = parseXfcc(headerValue);
    if (elements.length === 0) {
      throw new Error("Empty x-forwarded-client-cert header");
    }
    const element = selectElement === "first" ? elements[0] : elements[elements.length - 1];
    if (validate) {
      return validate(element);
    }
    const principal = element.subject ? extractCn(element.subject) : "";
    const claims = {};
    if (element.hash)
      claims.hash = element.hash;
    if (element.subject)
      claims.subject = element.subject;
    if (element.uri)
      claims.uri = element.uri;
    if (element.dns.length > 0)
      claims.dns = [...element.dns];
    if (element.by)
      claims.by = element.by;
    return new AuthContext(domain, true, principal, claims);
  };
}
function parseCertFromHeader(request, header) {
  const raw = request.headers.get(header);
  if (!raw) {
    throw new Error(`Missing ${header} header`);
  }
  const pemStr = decodeURIComponent(raw);
  if (!pemStr.startsWith("-----BEGIN CERTIFICATE-----")) {
    throw new Error("Header value is not a PEM certificate");
  }
  const { X509Certificate } = _loadNodeCrypto();
  try {
    return new X509Certificate(pemStr);
  } catch (exc) {
    throw new Error(`Failed to parse PEM certificate: ${exc}`);
  }
}
function checkCertExpiry(cert) {
  const now = new Date;
  const notBefore = new Date(cert.validFrom);
  const notAfter = new Date(cert.validTo);
  if (now < notBefore) {
    throw new Error("Certificate is not yet valid");
  }
  if (now > notAfter) {
    throw new Error("Certificate has expired");
  }
}
function mtlsAuthenticate(options) {
  const { validate, header = "X-SSL-Client-Cert", checkExpiry = false } = options;
  return async function authenticate(request) {
    const cert = parseCertFromHeader(request, header);
    if (checkExpiry) {
      checkCertExpiry(cert);
    }
    return validate(cert);
  };
}
var SUPPORTED_ALGORITHMS = new Set(["sha256", "sha1", "sha384", "sha512"]);
function mtlsAuthenticateFingerprint(options) {
  const { fingerprints, header, algorithm = "sha256", checkExpiry } = options;
  if (!SUPPORTED_ALGORITHMS.has(algorithm)) {
    throw new Error(`Unsupported hash algorithm: ${algorithm}`);
  }
  const entries = fingerprints instanceof Map ? fingerprints : new Map(Object.entries(fingerprints));
  function validate(cert) {
    const { createHash: createHash2 } = _loadNodeCrypto();
    const fp = createHash2(algorithm).update(cert.raw).digest("hex");
    const ctx = entries.get(fp);
    if (!ctx) {
      throw new Error(`Unknown certificate fingerprint: ${fp}`);
    }
    return ctx;
  }
  return mtlsAuthenticate({ validate, header, checkExpiry });
}
function mtlsAuthenticateSubject(options) {
  const { header, domain = "mtls", allowedSubjects = null, checkExpiry } = options ?? {};
  function validate(cert) {
    const subjectParts = cert.subject.split(`
`).map((s) => s.trim()).filter(Boolean);
    const subjectDn = subjectParts.join(", ");
    let cn = "";
    for (const part of subjectParts) {
      if (part.toUpperCase().startsWith("CN=")) {
        cn = part.slice(3);
        break;
      }
    }
    if (allowedSubjects !== null && !allowedSubjects.has(cn)) {
      throw new Error(`Subject CN '${cn}' not in allowed subjects`);
    }
    const serialHex = BigInt(`0x${cert.serialNumber}`).toString(16);
    const notValidAfter = new Date(cert.validTo).toISOString();
    return new AuthContext(domain, true, cn, {
      subject_dn: subjectDn,
      serial: serialHex,
      not_valid_after: notValidAfter
    });
  }
  return mtlsAuthenticate({ validate, header, checkExpiry });
}
// src/schema.ts
var str = utf8();
var bytes = binary();
var int = int64();
var int322 = int32();
var int162 = int16();
var int82 = int8();
var uint82 = uint8();
var uint162 = uint16();
var uint322 = uint32();
var uint642 = uint64();
var float = float64();
var float322 = float32();
var bool2 = bool();
function isField(x) {
  return x != null && typeof x.name === "string" && x.type != null && typeof x.nullable === "boolean";
}
function isDataType(x) {
  return x != null && typeof x.typeId === "number";
}
function toSchema(spec) {
  const maybeFields = spec.fields;
  if (Array.isArray(maybeFields)) {
    const out = [];
    for (const f of maybeFields) {
      if (isField(f)) {
        out.push(f);
      } else {
        out.push(field(f.name, f.type, f.nullable ?? true, f.metadata));
      }
    }
    return schema(out);
  }
  const fields = [];
  for (const [name, value] of Object.entries(spec)) {
    if (isField(value)) {
      fields.push(value);
    } else if (isDataType(value)) {
      fields.push(field(name, value, false));
    } else {
      throw new TypeError(`Invalid schema value for "${name}": expected DataType or Field, got ${typeof value}`);
    }
  }
  return schema(fields);
}
function inferParamTypes(spec) {
  const sch = toSchema(spec);
  if (sch.fields.length === 0)
    return;
  const result = {};
  for (const f of sch.fields) {
    let mapped;
    if (isUtf8(f.type))
      mapped = "str";
    else if (isBinary(f.type))
      mapped = "bytes";
    else if (isBool(f.type))
      mapped = "bool";
    else if (isFloat(f.type))
      mapped = "float";
    else if (isInt(f.type))
      mapped = "int";
    if (!mapped)
      return;
    result[f.name] = mapped;
  }
  return result;
}

// src/protocol.ts
var EMPTY_SCHEMA3 = schema([]);

class Protocol {
  name;
  protocolVersion;
  protocolVersionParts;
  _methods = new Map;
  constructor(name, options) {
    this.name = name;
    const raw = options?.protocolVersion;
    if (raw === undefined || raw === "") {
      this.protocolVersion = "";
      this.protocolVersionParts = null;
    } else {
      this.protocolVersion = raw;
      this.protocolVersionParts = parseProtocolVersion(raw);
    }
  }
  unary(name, config) {
    const params = toSchema(config.params);
    this._methods.set(name, {
      name,
      type: "unary" /* UNARY */,
      paramsSchema: params,
      resultSchema: toSchema(config.result),
      handler: config.handler,
      doc: config.doc,
      defaults: config.defaults,
      paramTypes: config.paramTypes ?? inferParamTypes(params)
    });
    return this;
  }
  producer(name, config) {
    const params = toSchema(config.params);
    this._methods.set(name, {
      name,
      type: "stream" /* STREAM */,
      paramsSchema: params,
      resultSchema: EMPTY_SCHEMA3,
      outputSchema: toSchema(config.outputSchema),
      inputSchema: EMPTY_SCHEMA3,
      producerInit: config.init,
      producerFn: config.produce,
      onCancel: config.onCancel,
      headerSchema: config.headerSchema ? toSchema(config.headerSchema) : undefined,
      headerInit: config.headerInit,
      doc: config.doc,
      defaults: config.defaults,
      paramTypes: config.paramTypes ?? inferParamTypes(params)
    });
    return this;
  }
  exchange(name, config) {
    const params = toSchema(config.params);
    this._methods.set(name, {
      name,
      type: "stream" /* STREAM */,
      paramsSchema: params,
      resultSchema: EMPTY_SCHEMA3,
      inputSchema: toSchema(config.inputSchema),
      outputSchema: toSchema(config.outputSchema),
      exchangeInit: config.init,
      exchangeFn: config.exchange,
      onCancel: config.onCancel,
      headerSchema: config.headerSchema ? toSchema(config.headerSchema) : undefined,
      headerInit: config.headerInit,
      doc: config.doc,
      defaults: config.defaults,
      paramTypes: config.paramTypes ?? inferParamTypes(params)
    });
    return this;
  }
  getMethods() {
    return new Map(this._methods);
  }
  getMethod(name) {
    return this._methods.get(name);
  }
  methodNames() {
    return [...this._methods.keys()].sort();
  }
}
// src/dispatch/stream.ts
var EMPTY_SCHEMA4 = schema([]);
async function dispatchStream(method, params, writer, reader, serverId, requestId, externalConfig, kind) {
  const isProducer = !!method.producerFn;
  let state;
  try {
    if (isProducer) {
      state = await method.producerInit(params);
    } else {
      state = await method.exchangeInit(params);
    }
  } catch (error) {
    const errSchema = method.headerSchema ?? EMPTY_SCHEMA4;
    const errBatch = buildErrorBatch(errSchema, error, serverId, requestId);
    await writer.writeStream(errSchema, [errBatch]);
    const inputSchema2 = await reader.openNextStream();
    if (inputSchema2) {
      while (await reader.readNextBatch() !== null) {}
    }
    return;
  }
  const outputSchema = state?.__outputSchema ?? method.outputSchema;
  const effectiveProducer = state?.__isProducer ?? isProducer;
  if (method.headerSchema && method.headerInit) {
    try {
      const headerOut = new OutputCollector(method.headerSchema, true, serverId, requestId, undefined, undefined, kind);
      const headerValues = method.headerInit(params, state, headerOut);
      const headerBatch = buildResultBatch(method.headerSchema, headerValues, serverId, requestId);
      const headerBatches = [...headerOut.batches.map((b) => b.batch), headerBatch];
      await writer.writeStream(method.headerSchema, headerBatches);
    } catch (error) {
      const errBatch = buildErrorBatch(method.headerSchema, error, serverId, requestId);
      await writer.writeStream(method.headerSchema, [errBatch]);
      const inputSchema2 = await reader.openNextStream();
      if (inputSchema2) {
        while (await reader.readNextBatch() !== null) {}
      }
      return;
    }
  }
  const inputSchema = await reader.openNextStream();
  if (!inputSchema) {
    const errBatch = buildErrorBatch(outputSchema, new Error("Expected input stream but got EOF"), serverId, requestId);
    await writer.writeStream(outputSchema, [errBatch]);
    return;
  }
  const stream = writer.openStream(outputSchema);
  const expectedInputSchema = state?.__inputSchema ?? method.inputSchema;
  try {
    while (true) {
      let inputBatch = await reader.readNextBatch();
      if (!inputBatch)
        break;
      if (inputBatch.metadata?.get(CANCEL_KEY)) {
        if (method.onCancel) {
          try {
            await method.onCancel(state);
          } catch (err2) {
            console.debug?.(`onCancel hook failed: ${err2 instanceof Error ? err2.message : err2}`);
          }
        }
        break;
      }
      if (expectedInputSchema && !effectiveProducer && inputBatch.schema !== expectedInputSchema) {
        try {
          inputBatch = conformBatchToSchema(inputBatch, expectedInputSchema);
        } catch (e) {
          if (e instanceof TypeError)
            throw e;
          console.debug?.(`Schema conformance skipped: ${e instanceof Error ? e.message : e}`);
        }
      }
      const out = new OutputCollector(outputSchema, effectiveProducer, serverId, requestId, undefined, undefined, kind);
      if (isProducer) {
        await method.producerFn(state, out);
      } else {
        await method.exchangeFn(state, inputBatch, out);
      }
      for (const emitted of out.batches) {
        let batch = emitted.batch;
        if (externalConfig) {
          batch = await maybeExternalizeBatch(batch, externalConfig);
        }
        if (emitted.metadata && emitted.metadata.size > 0) {
          batch = withBatchMetadata(batch, emitted.metadata);
        }
        await stream.write(batch);
      }
      if (out.finished) {
        break;
      }
    }
  } catch (error) {
    await stream.write(buildErrorBatch(outputSchema, error, serverId, requestId));
  }
  await stream.close();
  try {
    while (await reader.readNextBatch() !== null) {}
  } catch {}
}

// src/dispatch/unary.ts
async function dispatchUnary(method, params, writer, serverId, requestId, externalConfig, kind) {
  const schema2 = method.resultSchema;
  const out = new OutputCollector(schema2, true, serverId, requestId, undefined, undefined, kind);
  try {
    const result = await method.handler(params, out);
    let resultBatch = buildResultBatch(schema2, result, serverId, requestId);
    if (externalConfig) {
      resultBatch = await maybeExternalizeBatch(resultBatch, externalConfig);
    }
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    await writer.writeStream(schema2, batches);
  } catch (error) {
    const batch = buildErrorBatch(schema2, error, serverId, requestId);
    await writer.writeStream(schema2, [batch]);
  }
}

// src/server.ts
var EMPTY_SCHEMA5 = schema([]);
function randomStreamId() {
  const bytes2 = new Uint8Array(16);
  crypto.getRandomValues(bytes2);
  let out = "";
  for (let i = 0;i < bytes2.length; i++) {
    out += bytes2[i].toString(16).padStart(2, "0");
  }
  return out;
}

class VgiRpcServer {
  protocol;
  enableDescribe;
  serverId;
  _describePromise = null;
  protocolVersion;
  dispatchHook = null;
  externalConfig;
  onServeStart = null;
  serveStartFired = false;
  constructor(protocol, options) {
    this.protocol = protocol;
    this.enableDescribe = options?.enableDescribe ?? true;
    this.serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    this.dispatchHook = options?.dispatchHook ?? null;
    this.externalConfig = options?.externalLocation;
    this.protocolVersion = options?.protocolVersion ?? "";
    this.onServeStart = options?.onServeStart ?? null;
  }
  async notifyTransport(kind) {
    if (this.serveStartFired)
      return;
    if (this.onServeStart) {
      await this.onServeStart(kind);
    }
    this.serveStartFired = true;
  }
  async describeInfo() {
    if (!this._describePromise) {
      this._describePromise = buildDescribeBatch(this.protocol.name, this.protocol.getMethods(), this.serverId, this.protocol.protocolVersion || undefined).then(({ batch, metadata }) => ({
        batch,
        protocolHash: metadata.get("vgi_rpc.protocol_hash") ?? ""
      }));
    }
    return this._describePromise;
  }
  checkProtocolVersion(clientVersion) {
    const serverParts = this.protocol.protocolVersionParts;
    const serverVersion = this.protocol.protocolVersion;
    if (clientVersion === undefined) {
      throw new ProtocolVersionError(`VGI client/worker protocol_version mismatch.
` + `  Client: <not declared>
` + `  Server: ${serverVersion}
` + "  Direction: the client did not send a vgi_rpc.protocol_version " + "metadata key. This is either a vgi-rpc framework bug or a " + "non-VGI client connecting to a VGI worker.");
    }
    let clientParts;
    try {
      clientParts = parseProtocolVersion(clientVersion);
    } catch {
      throw new ProtocolVersionError(`VGI client/worker protocol_version mismatch.
` + `  Client: ${clientVersion}
` + `  Server: ${serverVersion}
` + "  Direction: client sent a malformed protocol_version. " + "Expected canonical semver MAJOR.MINOR.PATCH.");
    }
    if (clientParts[0] === serverParts[0] && clientParts[1] === serverParts[1]) {
      return;
    }
    const clientOlder = clientParts[0] < serverParts[0] || clientParts[0] === serverParts[0] && clientParts[1] < serverParts[1];
    const direction = clientOlder ? `client is too old; upgrade the VGI extension/client to a version supporting protocol_version ${serverVersion}.` : `server is too old; upgrade the VGI worker to a version supporting protocol_version ${clientVersion}.`;
    throw new ProtocolVersionError(`VGI client/worker protocol_version mismatch.
` + `  Client: ${clientVersion}
` + `  Server: ${serverVersion}
` + `  Direction: ${direction}`);
  }
  async run() {
    if (process.stdin.isTTY || process.stdout.isTTY) {
      process.stderr.write("WARNING: This process communicates via Arrow IPC on stdin/stdout " + `and is not intended to be run interactively.
` + "It should be launched as a subprocess by an RPC client " + `(e.g. vgi_rpc.connect()).
`);
    }
    const stdin = process.stdin;
    await this.serveConnection(stdin);
  }
  async serveConnection(readable, writable, transportKind = "pipe" /* PIPE */) {
    const reader = await IpcStreamReader.create(readable);
    const writer = new IpcStreamWriter(writable);
    try {
      while (true) {
        await this.notifyTransport(transportKind);
        await this.serveOne(reader, writer);
      }
    } catch (e) {
      if (e.message?.includes("closed") || e.message?.includes("Expected Schema Message") || e.message?.includes("null or length 0") || e.code === "EPIPE" || e.code === "ERR_STREAM_PREMATURE_CLOSE" || e.code === "ERR_STREAM_DESTROYED" || e instanceof Error && e.message.includes("EOF")) {
        return;
      }
      throw e;
    } finally {
      await reader.cancel();
    }
  }
  async serveOne(reader, writer) {
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }
    const { schema: schema2, batches } = stream;
    if (batches.length === 0) {
      const err2 = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA5, err2, this.serverId, null);
      await writer.writeStream(EMPTY_SCHEMA5, [errBatch]);
      return;
    }
    const batch = batches[0];
    let methodName;
    let params;
    let requestId;
    try {
      const parsed = parseRequest(schema2, batch);
      methodName = parsed.methodName;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA5, e, this.serverId, null);
      await writer.writeStream(EMPTY_SCHEMA5, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError) {
        return;
      }
      throw e;
    }
    if (methodName === DESCRIBE_METHOD_NAME && this.enableDescribe) {
      const { batch: batch2 } = await this.describeInfo();
      await writer.writeStream(batch2.schema, [batch2]);
      return;
    }
    const method = this.protocol.getMethod(methodName);
    if (!method) {
      const available = this.protocol.methodNames();
      const err2 = new MethodNotImplementedError(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      const errBatch = buildErrorBatch(EMPTY_SCHEMA5, err2, this.serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA5, [errBatch]);
      return;
    }
    if (this.protocol.protocolVersionParts !== null) {
      try {
        const md = batch.metadata;
        this.checkProtocolVersion(md?.get(PROTOCOL_VERSION_KEY));
      } catch (exc) {
        const errSchema = method.type === "unary" /* UNARY */ ? method.resultSchema : EMPTY_SCHEMA5;
        const errBatch = buildErrorBatch(errSchema, exc, this.serverId, requestId);
        await writer.writeStream(errSchema, [errBatch]);
        return;
      }
    }
    const methodType = method.type === "unary" /* UNARY */ ? "unary" : "stream";
    let requestData;
    if (this.dispatchHook) {
      try {
        requestData = serializeBatch(batch);
      } catch {}
    }
    let streamId;
    if (methodType === "stream") {
      streamId = randomStreamId();
    }
    const { protocolHash } = await this.describeInfo();
    const info = {
      method: methodName,
      methodType,
      serverId: this.serverId,
      requestId,
      protocol: this.protocol.name,
      protocolHash,
      protocolVersion: this.protocolVersion,
      kind: "pipe" /* PIPE */,
      principal: "",
      authDomain: "",
      authenticated: false,
      remoteAddr: "",
      requestData,
      streamId
    };
    const stats = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0
    };
    const token = this.dispatchHook?.onDispatchStart(info);
    let dispatchError;
    applyDefaults(params, method.defaults);
    try {
      if (method.type === "unary" /* UNARY */) {
        await dispatchUnary(method, params, writer, this.serverId, requestId, this.externalConfig, "pipe" /* PIPE */);
      } else {
        await dispatchStream(method, params, writer, reader, this.serverId, requestId, this.externalConfig, "pipe" /* PIPE */);
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      this.dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
}

// src/serve-stream.ts
//! Serve a protocol over a caller-provided byte-stream pair — the stream
//! sibling of `serveTcp` / `serveUnix`, with no socket/listener of its own.
//!
//! Useful for transports the launcher helpers don't cover: a Web Worker /
//! `MessagePort` bridge (postMessage), an in-memory pipe, or a pre-connected
//! socket. The host side already has this symmetry via `pipeConnect`.
async function serveStream(protocol, options) {
  const server = new VgiRpcServer(protocol, options.serverOptions);
  await server.serveConnection(options.readable, options.writable, options.transportKind);
}
// src/wire/public.ts
async function readRequest(data, externalConfig) {
  let batch = deserializeBatch(data);
  if (externalConfig) {
    batch = await resolveExternalLocation(batch, externalConfig);
  }
  const parsed = parseRequest(batch.schema, batch);
  return { methodName: parsed.methodName, params: parsed.params, schema: batch.schema };
}
function writeRequest(methodName, paramsSchema, params, protocolVersion) {
  return buildRequestIpc(paramsSchema, params, methodName, { protocolVersion });
}
function buildErrorStream(error, schema2, serverId = "", requestId = null) {
  const streamSchema = schema2 ?? schema([]);
  return serializeIpcStream(streamSchema, [buildErrorBatch(streamSchema, error, serverId, requestId)]);
}
function findStateToken(data) {
  return findBatchMetadataValue(data, STATE_KEY);
}
function findProtocolVersion(data) {
  return findBatchMetadataValue(data, PROTOCOL_VERSION_KEY);
}
async function findBatchMetadataValue(data, key) {
  try {
    const reader = await readSequentialStreams(data);
    while (true) {
      const stream = await reader.readStream();
      if (!stream)
        return null;
      for (const batch of stream.batches) {
        const value = batch.metadata?.get(key);
        if (value)
          return value;
      }
    }
  } catch {
    return null;
  }
}
async function readUnaryResult(data) {
  const reader = await readSequentialStreams(data);
  const stream = await reader.readStream();
  if (!stream)
    return null;
  for (const batch of stream.batches) {
    if (batch.numRows > 0) {
      const column = batch.getChild("result");
      if (!column)
        return null;
      const value = column.get(0);
      if (value == null)
        return null;
      return { envelopeSchema: batch.schema, resultBytes: value };
    }
    if (!batch.metadata?.has(LOG_LEVEL_KEY))
      return null;
  }
  return null;
}
function writeUnaryResult(envelopeSchema, resultBytes) {
  const batch = envelopeSchema.fields.length === 0 ? emptyBatchWithMetadata(envelopeSchema, new Map) : singleRowBatchWithMetadata(envelopeSchema, { result: resultBytes }, new Map);
  return serializeIpcStream(envelopeSchema, [batch]);
}
// src/launcher/hash.ts
var HASH_LEN = 16;
function canonicalJson(value) {
  if (value === null)
    return "null";
  if (typeof value === "boolean")
    return value ? "true" : "false";
  if (typeof value === "number") {
    return JSON.stringify(value);
  }
  if (typeof value === "string")
    return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (typeof value === "object") {
    const keys = Object.keys(value).sort();
    const parts = keys.map((k) => `${JSON.stringify(k)}:${canonicalJson(value[k])}`);
    return `{${parts.join(",")}}`;
  }
  throw new TypeError(`canonicalJson: unsupported type ${typeof value}`);
}
async function sha256Hex3(data) {
  const buf2 = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf2).set(data);
  const digest = await crypto.subtle.digest("SHA-256", buf2);
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
}
async function computeHash(workerArgv, cwd, env) {
  const cwdValue = cwd !== undefined ? cwd : process.cwd();
  const sourceEnv = env ?? process.env;
  const filteredEnv = {};
  for (const key of Object.keys(sourceEnv)) {
    if (key.startsWith("VGI_RPC_")) {
      const v = sourceEnv[key];
      if (v !== undefined)
        filteredEnv[key] = v;
    }
  }
  const canonical = {
    cmd: [...workerArgv],
    cwd: cwdValue,
    env: filteredEnv
  };
  const payload = new TextEncoder().encode(canonicalJson(canonical));
  const hex = await sha256Hex3(payload);
  return hex.slice(0, HASH_LEN);
}
// src/launcher/launch.ts
import { spawn } from "node:child_process";
import { createWriteStream, unlinkSync as unlinkSync2 } from "node:fs";

// src/launcher/lock.ts
import { closeSync, constants as FS, openSync, readSync, statSync, writeSync } from "node:fs";
var POLL_MS = 50;
var VERIFY_RETRIES = 5;
function pidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0)
    return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err2) {
    return err2?.code === "EPERM";
  }
}
function readPid(path) {
  try {
    const fd = openSync(path, FS.O_RDONLY);
    try {
      const buf2 = Buffer.alloc(64);
      const n = readSync(fd, buf2, 0, buf2.length, 0);
      const text = buf2.subarray(0, n).toString("utf8").trim();
      if (text === "")
        return 0;
      const parsed = Number(text);
      return Number.isInteger(parsed) ? parsed : 0;
    } finally {
      closeSync(fd);
    }
  } catch {
    return 0;
  }
}
function tryStampPid(path) {
  const fd = openSync(path, FS.O_RDWR | FS.O_CREAT, 384);
  try {
    const stamp = Buffer.from(String(process.pid), "utf8");
    const { ftruncateSync } = __require("node:fs");
    ftruncateSync(fd, 0);
    let written = 0;
    while (written < stamp.length) {
      const n = writeSync(fd, stamp, written, stamp.length - written, 0 + written);
      if (n <= 0)
        throw new Error(`writeSync returned ${n}`);
      written += n;
    }
    const st = statSync(path);
    if (st.size !== stamp.length)
      return false;
    return true;
  } finally {
    closeSync(fd);
  }
}
function clearStamp(path) {
  try {
    const fd = openSync(path, FS.O_RDWR);
    try {
      const { ftruncateSync } = __require("node:fs");
      ftruncateSync(fd, 0);
    } finally {
      closeSync(fd);
    }
  } catch {}
}
function tryAcquireLock(lockPath) {
  for (let attempt = 0;attempt < VERIFY_RETRIES; attempt++) {
    const existingPid = readPid(lockPath);
    if (existingPid > 0 && pidAlive(existingPid)) {
      return null;
    }
    if (!tryStampPid(lockPath))
      continue;
    const verifyPid = readPid(lockPath);
    if (verifyPid !== process.pid) {
      continue;
    }
    let released = false;
    return {
      path: lockPath,
      release() {
        if (released)
          return;
        released = true;
        clearStamp(lockPath);
      }
    };
  }
  return null;
}
async function acquireLock(lockPath, timeoutMs) {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  for (;; ) {
    const handle = tryAcquireLock(lockPath);
    if (handle)
      return handle;
    if (Date.now() >= deadline) {
      throw new Error(`failed to acquire ${lockPath} within ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

// src/launcher/state.ts
import { existsSync, mkdirSync, readFileSync, statSync as statSync2, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
function socketPaths(stateDir, hashId) {
  return {
    lockPath: path.join(stateDir, `${hashId}.lock`),
    sockPath: path.join(stateDir, `${hashId}.sock`),
    metaPath: path.join(stateDir, `${hashId}.meta`)
  };
}
function defaultStateDir() {
  let base;
  if (process.platform === "win32") {
    base = path.join(tmpdir(), "vgi-rpc");
  } else {
    const xdg = process.env.XDG_RUNTIME_DIR;
    if (xdg) {
      base = path.join(xdg, "vgi-rpc");
    } else {
      const uid = typeof process.geteuid === "function" ? process.geteuid() : 0;
      base = path.join(tmpdir(), `vgi-rpc-${uid}`);
    }
  }
  mkdirSync(base, { recursive: true, mode: 448 });
  if (process.platform !== "win32" && typeof process.geteuid === "function") {
    try {
      const st = statSync2(base);
      if (st.uid !== process.geteuid()) {
        throw new Error(`state directory ${base} is not owned by current user`);
      }
    } catch (err2) {
      if (err2?.code === "ENOENT") {} else {
        throw err2;
      }
    }
  }
  return base;
}
function writeMeta(metaPath, workerArgv, cwd, sockPath) {
  const payload = {
    cmd: [...workerArgv],
    cwd,
    socket: sockPath,
    started_at: Date.now() / 1000,
    launcher_pid: process.pid
  };
  try {
    writeFileSync(metaPath, JSON.stringify(payload, null, 2), { encoding: "utf8", mode: 384 });
  } catch {}
}
async function probeSocket(sockPath, timeoutMs = 2000) {
  if (!existsSync(sockPath))
    return false;
  const net = await import("node:net");
  return new Promise((resolve) => {
    const sock = net.createConnection({ path: sockPath });
    const timer = setTimeout(() => {
      sock.destroy();
      resolve(false);
    }, timeoutMs);
    sock.once("connect", () => {
      clearTimeout(timer);
      sock.end();
      resolve(true);
    });
    sock.once("error", () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
}
function tryReadMeta(metaPath) {
  try {
    const raw = readFileSync(metaPath, "utf8");
    const meta = JSON.parse(raw);
    return {
      cmd: Array.isArray(meta.cmd) ? meta.cmd.map(String) : [],
      cwd: typeof meta.cwd === "string" ? meta.cwd : "",
      startedAt: typeof meta.started_at === "number" ? meta.started_at : null
    };
  } catch {
    return { cmd: [], cwd: "", startedAt: null };
  }
}
async function statusRows(stateDir) {
  const { readdirSync } = await import("node:fs");
  const rows = [];
  let entries;
  try {
    entries = readdirSync(stateDir);
  } catch {
    return rows;
  }
  for (const name of entries.sort()) {
    if (!name.endsWith(".lock"))
      continue;
    const hashId = name.slice(0, -5);
    const { sockPath, metaPath } = socketPaths(stateDir, hashId);
    const meta = tryReadMeta(metaPath);
    rows.push({
      hashId,
      cmd: meta.cmd,
      cwd: meta.cwd,
      socket: sockPath,
      startedAt: meta.startedAt,
      alive: await probeSocket(sockPath)
    });
  }
  return rows;
}
async function gcStateDir(stateDir, tryAcquire, options) {
  const { readdirSync } = await import("node:fs");
  const cleaned = [];
  const skipped = [];
  const limit = options?.limit ?? null;
  const excludeHash = options?.excludeHash ?? null;
  let entries;
  try {
    entries = readdirSync(stateDir);
  } catch {
    return { cleaned, skippedInUse: skipped };
  }
  let seen = 0;
  for (const name of entries.sort()) {
    if (!name.endsWith(".lock"))
      continue;
    if (limit !== null && seen >= limit)
      break;
    seen += 1;
    const hashId = name.slice(0, -5);
    if (excludeHash !== null && hashId === excludeHash)
      continue;
    const { lockPath, sockPath, metaPath } = socketPaths(stateDir, hashId);
    const release = await tryAcquire(lockPath);
    if (release === null) {
      skipped.push(hashId);
      continue;
    }
    try {
      if (await probeSocket(sockPath)) {
        continue;
      }
      for (const p of [sockPath, metaPath, lockPath]) {
        try {
          unlinkSync(p);
        } catch {}
      }
      cleaned.push(hashId);
    } finally {
      release();
    }
  }
  return { cleaned, skippedInUse: skipped };
}

// src/launcher/launch.ts
var DEFAULT_GC_LIMIT = 16;
async function launch(config) {
  if (!config.workerArgv || config.workerArgv.length === 0) {
    throw new Error("workerArgv must be non-empty");
  }
  const stateDir = config.stateDir ?? defaultStateDir();
  const idleTimeout = config.idleTimeout ?? 300;
  const connectTimeoutMs = (config.connectTimeout ?? 30) * 1000;
  const startupTimeoutMs = (config.workerStartupTimeout ?? 60) * 1000;
  let lockPath;
  let sockPath;
  let metaPath;
  let hashId;
  if (config.socketPath !== undefined) {
    const { resolve } = await import("node:path");
    sockPath = resolve(config.socketPath);
    lockPath = `${sockPath}.lock`;
    metaPath = null;
    hashId = null;
  } else {
    hashId = await computeHash(config.workerArgv);
    const paths = socketPaths(stateDir, hashId);
    lockPath = paths.lockPath;
    sockPath = paths.sockPath;
    metaPath = paths.metaPath;
  }
  const handle = await acquireLock(lockPath, connectTimeoutMs);
  try {
    if (await probeSocket(sockPath)) {
      return sockPath;
    }
    try {
      unlinkSync2(sockPath);
    } catch {}
    if (metaPath !== null) {
      writeMeta(metaPath, config.workerArgv, process.cwd(), sockPath);
    }
    await spawnWorker(config.workerArgv, sockPath, idleTimeout, config.workerStderr ?? null, startupTimeoutMs);
    return sockPath;
  } finally {
    handle.release();
    if (hashId !== null) {
      try {
        await gcStateDir(stateDir, async (p) => {
          const h = tryAcquireLock(p);
          return h ? () => h.release() : null;
        }, { limit: DEFAULT_GC_LIMIT, excludeHash: hashId });
      } catch {}
    }
  }
}
async function spawnWorker(workerArgv, sockPath, idleTimeout, workerStderr, startupTimeoutMs) {
  const fullArgv = [...workerArgv, "--unix", sockPath, "--idle-timeout", String(idleTimeout)];
  const [cmd, ...rest] = fullArgv;
  const stderrTarget = workerStderr === null ? "ignore" : "pipe";
  const proc = spawn(cmd, rest, {
    stdio: ["ignore", "pipe", stderrTarget],
    detached: false
  });
  if (workerStderr !== null && proc.stderr) {
    const sink = createWriteStream(workerStderr, { flags: "a" });
    proc.stderr.pipe(sink);
  }
  const expectedPrefix = `UNIX:${sockPath}`;
  const reader = lineReader(proc.stdout);
  const deadline = Date.now() + startupTimeoutMs;
  while (Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const result = await Promise.race([
      reader.next().then((r) => ({ kind: "line", value: r })),
      onceExit(proc).then((rc) => ({ kind: "exit", rc })),
      delay(remaining).then(() => ({ kind: "timeout" }))
    ]);
    if (result.kind === "exit") {
      throw new Error(`worker exited before readiness (rc=${result.rc})`);
    }
    if (result.kind === "timeout") {
      proc.kill("SIGTERM");
      throw new Error(`worker did not emit UNIX:<path> within ${startupTimeoutMs}ms`);
    }
    if (result.value.done) {
      const rc = await onceExit(proc);
      throw new Error(`worker exited before readiness (rc=${rc})`);
    }
    const line = result.value.value;
    if (line.startsWith("UNIX:")) {
      if (line !== expectedPrefix) {
        proc.kill("SIGTERM");
        throw new Error(`worker bound to unexpected path: ${JSON.stringify(line)} (expected ${JSON.stringify(expectedPrefix)})`);
      }
      reader.drainAndDiscard();
      return;
    }
    process.env.VGI_RPC_LAUNCHER_DEBUG && process.stderr.write(`launcher: skipping pre-bind stdout line: ${JSON.stringify(line)}
`);
  }
  proc.kill("SIGTERM");
  throw new Error(`worker did not emit UNIX:<path> within ${startupTimeoutMs}ms`);
}
function lineReader(stream) {
  let buffer = "";
  let ended = false;
  const queued = [];
  const waiters = [];
  let discardMode = false;
  const flushWaiter = () => {
    if (waiters.length === 0)
      return;
    if (queued.length > 0) {
      const w = waiters.shift();
      w?.({ done: false, value: queued.shift() ?? "" });
    } else if (ended) {
      const w = waiters.shift();
      w?.({ done: true, value: "" });
    }
  };
  stream.setEncoding?.("utf8");
  stream.on("data", (chunk) => {
    if (discardMode)
      return;
    buffer += String(chunk);
    for (;; ) {
      const nl = buffer.indexOf(`
`);
      if (nl < 0)
        break;
      const line = buffer.slice(0, nl).replace(/\r$/, "");
      buffer = buffer.slice(nl + 1);
      queued.push(line);
    }
    flushWaiter();
  });
  stream.on("end", () => {
    ended = true;
    if (buffer.length > 0) {
      queued.push(buffer.replace(/\r$/, ""));
      buffer = "";
    }
    flushWaiter();
  });
  stream.on("error", () => {
    ended = true;
    flushWaiter();
  });
  return {
    next() {
      return new Promise((resolve) => {
        waiters.push(resolve);
        flushWaiter();
      });
    },
    drainAndDiscard() {
      discardMode = true;
      queued.length = 0;
      stream.resume?.();
    }
  };
}
function onceExit(proc) {
  return new Promise((resolve) => {
    proc.once("exit", (code) => resolve(code));
  });
}
function delay(ms) {
  return new Promise((r) => setTimeout(r, Math.max(0, ms)));
}
// src/launcher/serve-tcp.ts
import { createServer } from "node:net";
var EMPTY_SCHEMA6 = schema([]);
async function serveTcp(protocol, options = {}) {
  const host = options.host ?? "127.0.0.1";
  const requestedPort = options.port ?? 0;
  const idleTimeoutS = options.idleTimeout ?? 300;
  const startupGraceS = options.startupGraceSeconds ?? 5;
  const protocolVersion = options.protocolVersion ?? "";
  const serverId = options.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
  const enableDescribe = options.enableDescribe ?? true;
  const dispatchHook = options.dispatchHook ?? null;
  const externalConfig = options.externalLocation;
  const onServeStart = options.onServeStart ?? null;
  const backlog = options.backlog ?? 128;
  const announcementSink = options.announcementSink ?? process.stdout;
  let describePromise = null;
  function describeInfo() {
    if (!describePromise) {
      describePromise = buildDescribeBatch(protocol.name, protocol.getMethods(), serverId).then(({ batch, metadata }) => ({
        batch,
        protocolHash: metadata.get("vgi_rpc.protocol_hash") ?? ""
      }));
    }
    return describePromise;
  }
  let serveStartFired = false;
  async function notifyTransport() {
    if (serveStartFired)
      return;
    if (onServeStart) {
      await onServeStart("tcp" /* TCP */);
    }
    serveStartFired = true;
  }
  const server = createServer({ allowHalfOpen: false });
  let activeConnections = 0;
  let idleTimer = null;
  let resolveDone = () => {};
  let rejectDone = () => {};
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  let stopped = false;
  function armIdleTimer() {
    if (idleTimeoutS <= 0)
      return;
    if (idleTimer)
      clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      if (activeConnections === 0 && !stopped) {
        shutdown();
      }
    }, idleTimeoutS * 1000);
  }
  function disarmIdleTimer() {
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = null;
    }
  }
  async function shutdown() {
    if (stopped)
      return;
    stopped = true;
    disarmIdleTimer();
    await new Promise((resolve) => {
      server.close(() => resolve());
    });
    resolveDone();
  }
  server.on("connection", (socket) => {
    try {
      socket.setNoDelay(true);
    } catch {}
    activeConnections += 1;
    disarmIdleTimer();
    handleConnection(socket).catch((err2) => {
      process.stderr.write(`vgi-rpc/tcp: connection failed: ${err2?.message ?? err2}
`);
    }).finally(() => {
      activeConnections -= 1;
      socket.destroy();
      if (activeConnections === 0 && !stopped) {
        armIdleTimer();
      }
    });
  });
  server.on("error", (err2) => {
    if (stopped)
      return;
    rejectDone(err2);
  });
  async function handleConnection(socket) {
    const reader = await IpcStreamReader.create(socket);
    const writer = new IpcStreamWriter(socket);
    try {
      await notifyTransport();
      while (true) {
        try {
          await serveOnce(reader, writer);
        } catch (e) {
          const err2 = e;
          if (err2?.message?.includes("closed") || err2?.message?.includes("Expected Schema Message") || err2?.message?.includes("null or length 0") || err2?.message?.includes("EOF") || err2?.code === "EPIPE" || err2?.code === "ERR_STREAM_PREMATURE_CLOSE" || err2?.code === "ERR_STREAM_DESTROYED") {
            return;
          }
          throw e;
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {}
    }
  }
  async function serveOnce(reader, writer) {
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }
    const { schema: schema2, batches } = stream;
    if (batches.length === 0) {
      const err2 = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA6, err2, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA6, [errBatch]);
      return;
    }
    const batch = batches[0];
    let methodName;
    let params;
    let requestId;
    try {
      const parsed = parseRequest(schema2, batch);
      methodName = parsed.methodName;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA6, e, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA6, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError)
        return;
      throw e;
    }
    if (methodName === DESCRIBE_METHOD_NAME && enableDescribe) {
      const { batch: descBatch } = await describeInfo();
      await writer.writeStream(descBatch.schema, [descBatch]);
      return;
    }
    const methods = protocol.getMethods();
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err2 = new Error(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      const errBatch = buildErrorBatch(EMPTY_SCHEMA6, err2, serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA6, [errBatch]);
      return;
    }
    const methodType = method.type === "unary" /* UNARY */ ? "unary" : "stream";
    let requestData;
    try {
      requestData = serializeBatch(batch);
    } catch {}
    const { protocolHash } = await describeInfo();
    const info = {
      method: methodName,
      methodType,
      serverId,
      requestId,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: "tcp" /* TCP */,
      principal: "",
      authDomain: "",
      authenticated: false,
      remoteAddr: "",
      requestData
    };
    const stats = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0
    };
    const token = dispatchHook?.onDispatchStart(info);
    let dispatchError;
    applyDefaults(params, method.defaults);
    try {
      if (method.type === "unary" /* UNARY */) {
        await dispatchUnary(method, params, writer, serverId, requestId, externalConfig, "tcp" /* TCP */);
      } else {
        await dispatchStream(method, params, writer, reader, serverId, requestId, externalConfig, "tcp" /* TCP */);
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
  await new Promise((resolve, reject) => {
    server.listen({ host, port: requestedPort, backlog }, () => resolve());
    server.once("error", (err2) => reject(err2));
  });
  const address = server.address();
  const boundPort = typeof address === "object" && address ? address.port : requestedPort;
  options.onBound?.(host, boundPort);
  announcementSink.write(`TCP:${host}:${boundPort}
`);
  if (idleTimeoutS > 0) {
    setTimeout(() => {
      if (activeConnections === 0 && !stopped)
        armIdleTimer();
    }, startupGraceS * 1000).unref?.();
  }
  return {
    host,
    port: boundPort,
    stop: shutdown,
    done
  };
}
// src/launcher/serve-unix.ts
import { existsSync as existsSync2, unlinkSync as unlinkSync3 } from "node:fs";
import { createServer as createServer2 } from "node:net";
import * as path2 from "node:path";
var EMPTY_SCHEMA7 = schema([]);
async function serveUnix(protocol, options) {
  const sockPath = path2.resolve(options.unixPath);
  const idleTimeoutS = options.idleTimeout ?? 300;
  const startupGraceS = options.startupGraceSeconds ?? 5;
  const protocolVersion = options.protocolVersion ?? "";
  const serverId = options.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
  const enableDescribe = options.enableDescribe ?? true;
  const dispatchHook = options.dispatchHook ?? null;
  const externalConfig = options.externalLocation;
  const onServeStart = options.onServeStart ?? null;
  const backlog = options.backlog ?? 16;
  const announcementSink = options.announcementSink ?? process.stdout;
  if (existsSync2(sockPath)) {
    try {
      unlinkSync3(sockPath);
    } catch {}
  }
  let describePromise = null;
  function describeInfo() {
    if (!describePromise) {
      describePromise = buildDescribeBatch(protocol.name, protocol.getMethods(), serverId).then(({ batch, metadata }) => ({
        batch,
        protocolHash: metadata.get("vgi_rpc.protocol_hash") ?? ""
      }));
    }
    return describePromise;
  }
  let serveStartFired = false;
  async function notifyTransport() {
    if (serveStartFired)
      return;
    if (onServeStart) {
      await onServeStart("unix" /* UNIX */);
    }
    serveStartFired = true;
  }
  const server = createServer2({ allowHalfOpen: false });
  let activeConnections = 0;
  let idleTimer = null;
  let resolveDone = () => {};
  let rejectDone = () => {};
  const done = new Promise((resolve2, reject) => {
    resolveDone = resolve2;
    rejectDone = reject;
  });
  let stopped = false;
  function armIdleTimer() {
    if (idleTimeoutS <= 0)
      return;
    if (idleTimer)
      clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      if (activeConnections === 0 && !stopped) {
        shutdown();
      }
    }, idleTimeoutS * 1000);
  }
  function disarmIdleTimer() {
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = null;
    }
  }
  async function shutdown() {
    if (stopped)
      return;
    stopped = true;
    disarmIdleTimer();
    await new Promise((resolve2) => {
      server.close(() => resolve2());
    });
    try {
      unlinkSync3(sockPath);
    } catch {}
    resolveDone();
  }
  server.on("connection", (socket) => {
    activeConnections += 1;
    disarmIdleTimer();
    handleConnection(socket).catch((err2) => {
      process.stderr.write(`vgi-rpc/unix: connection failed: ${err2?.message ?? err2}
`);
    }).finally(() => {
      activeConnections -= 1;
      socket.destroy();
      if (activeConnections === 0 && !stopped) {
        armIdleTimer();
      }
    });
  });
  server.on("error", (err2) => {
    if (stopped)
      return;
    rejectDone(err2);
  });
  async function handleConnection(socket) {
    const reader = await IpcStreamReader.create(socket);
    const writer = new IpcStreamWriter(socket);
    try {
      await notifyTransport();
      while (true) {
        try {
          await serveOnce(reader, writer);
        } catch (e) {
          const err2 = e;
          if (err2?.message?.includes("closed") || err2?.message?.includes("Expected Schema Message") || err2?.message?.includes("null or length 0") || err2?.message?.includes("EOF") || err2?.code === "EPIPE" || err2?.code === "ERR_STREAM_PREMATURE_CLOSE" || err2?.code === "ERR_STREAM_DESTROYED") {
            return;
          }
          throw e;
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {}
    }
  }
  async function serveOnce(reader, writer) {
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }
    const { schema: schema2, batches } = stream;
    if (batches.length === 0) {
      const err2 = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA7, err2, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA7, [errBatch]);
      return;
    }
    const batch = batches[0];
    let methodName;
    let params;
    let requestId;
    try {
      const parsed = parseRequest(schema2, batch);
      methodName = parsed.methodName;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA7, e, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA7, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError)
        return;
      throw e;
    }
    if (methodName === DESCRIBE_METHOD_NAME && enableDescribe) {
      const { batch: descBatch } = await describeInfo();
      await writer.writeStream(descBatch.schema, [descBatch]);
      return;
    }
    const methods = protocol.getMethods();
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err2 = new Error(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      const errBatch = buildErrorBatch(EMPTY_SCHEMA7, err2, serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA7, [errBatch]);
      return;
    }
    const methodType = method.type === "unary" /* UNARY */ ? "unary" : "stream";
    let requestData;
    try {
      requestData = serializeBatch(batch);
    } catch {}
    const { protocolHash } = await describeInfo();
    const info = {
      method: methodName,
      methodType,
      serverId,
      requestId,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: "unix" /* UNIX */,
      principal: "",
      authDomain: "",
      authenticated: false,
      remoteAddr: "",
      requestData
    };
    const stats = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0
    };
    const token = dispatchHook?.onDispatchStart(info);
    let dispatchError;
    applyDefaults(params, method.defaults);
    try {
      if (method.type === "unary" /* UNARY */) {
        await dispatchUnary(method, params, writer, serverId, requestId, externalConfig, "unix" /* UNIX */);
      } else {
        await dispatchStream(method, params, writer, reader, serverId, requestId, externalConfig, "unix" /* UNIX */);
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
  await new Promise((resolve2, reject) => {
    server.listen({ path: sockPath, backlog }, () => resolve2());
    server.once("error", (err2) => reject(err2));
  });
  try {
    const { chmodSync } = await import("node:fs");
    chmodSync(sockPath, 384);
  } catch {}
  options.onBound?.(sockPath);
  announcementSink.write(`UNIX:${sockPath}
`);
  if (idleTimeoutS > 0) {
    setTimeout(() => {
      if (activeConnections === 0 && !stopped)
        armIdleTimer();
    }, startupGraceS * 1000).unref?.();
  }
  return {
    socketPath: sockPath,
    stop: shutdown,
    done
  };
}
export {
  writeUnaryResult,
  writeRequest,
  unpackStateToken,
  uint82 as uint8,
  uint642 as uint64,
  uint322 as uint32,
  uint162 as uint16,
  tryAcquireLock,
  tokenDigest,
  toSchema,
  tcpConnect,
  subprocessConnect,
  str,
  statusRows,
  socketPaths,
  serveUnix,
  serveTcp,
  serveStream,
  resolveExternalLocation,
  redactClaims,
  readUnaryResult,
  readRequest,
  probeSocket,
  pipeConnect,
  parseXfcc,
  parseUseIdTokenAsBearer,
  parseResourceMetadataUrl,
  parseDeviceCodeClientSecret,
  parseDeviceCodeClientId,
  parseDescribeResponse,
  parseClientSecret,
  parseClientId,
  otelTraceContext,
  oauthResourceMetadataToJson,
  noRedaction,
  mtlsAuthenticateXfcc,
  mtlsAuthenticateSubject,
  mtlsAuthenticateFingerprint,
  mtlsAuthenticate,
  maybeExternalizeBatch,
  makeExternalLocationBatch,
  computeHash as launcherComputeHash,
  launch,
  jwtAuthenticate,
  jsonStateSerializer,
  isExternalLocationBatch,
  int82 as int8,
  int322 as int32,
  int162 as int16,
  int,
  inferParamTypes,
  httpsOnlyValidator,
  httpOAuthMetadata,
  httpIntrospect,
  httpConnect,
  gcStateDir,
  float322 as float32,
  float,
  findStateToken,
  findProtocolVersion,
  fetchOAuthMetadata,
  defaultStateDir,
  decodeContentEncoding,
  createIntrospector,
  createHttpHandler,
  chainAuthenticate,
  bytes,
  buildErrorStream,
  bool2 as bool,
  bearerAuthenticateStatic,
  bearerAuthenticate,
  acquireLock,
  VgiRpcServer,
  VersionError,
  UPLOAD_URL_RESPONSE_SCHEMA,
  UPLOAD_URL_PARAMS_SCHEMA,
  UPLOAD_URL_METHOD,
  TransportKind,
  SessionLostError,
  ServerDrainingError,
  STATE_KEY,
  SERVER_ID_KEY,
  RpcError,
  RPC_METHOD_KEY,
  RPC_ERROR_HEADER,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  REQUEST_ID_KEY,
  REDACTED,
  Protocol,
  PipeStreamSession,
  PROTOCOL_NAME_KEY,
  OutputCollector,
  MethodType,
  MethodNotImplementedError,
  MAX_UPLOAD_URL_COUNT,
  LOG_MESSAGE_KEY,
  LOG_LEVEL_KEY,
  LOG_EXTRA_KEY,
  INTROSPECT_ENDPOINT,
  INTROSPECT_ENABLED_HEADER,
  HttpStreamSession,
  FdSink,
  ERROR_KIND_SESSION_LOST,
  ERROR_KIND_SERVER_DRAINING,
  ERROR_KIND_METHOD_NOT_IMPLEMENTED,
  ERROR_KIND_KEY,
  DESCRIBE_VERSION_KEY,
  DESCRIBE_VERSION,
  DESCRIBE_METHOD_NAME,
  DEFAULT_INTROSPECT_TTL_SECONDS,
  AuthUnavailableError,
  AuthReason,
  AuthFailure,
  AuthContext,
  AccessLogSampler,
  AccessLogHook,
  AUTH_REASON_HEADER,
  AUTH_PROXY_REQUIRED_HEADER,
  ARROW_CONTENT_TYPE
};

//# debugId=69D479CC9443677764756E2164756E21
