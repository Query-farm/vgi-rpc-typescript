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
// src/access-log.ts
var _NODE_FS_MOD = "node:fs";
function _loadWriteSync() {
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req) {
    throw new Error("FdSink requires Node.js or Bun (node:fs.writeSync). For other runtimes, " + "supply a custom AccessLogSink that wraps console.log or your logger.");
  }
  return req(_NODE_FS_MOD).writeSync;
}

class FdSink {
  fd;
  _writeSync = _loadWriteSync();
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

class AccessLogHook {
  sink;
  serverVersion;
  level;
  constructor(sink, options = {}) {
    this.sink = sink;
    if (typeof options === "string") {
      this.serverVersion = options;
      this.level = "INFO";
    } else {
      this.serverVersion = options.serverVersion ?? "";
      this.level = options.level ?? "INFO";
    }
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
    if (info.requestData && info.requestData.length > 0) {
      const encoded = base64(info.requestData);
      if (this.level === "DEBUG") {
        rec.request_data = encoded;
      } else {
        rec.original_request_bytes = encoded.length;
        rec.truncated = true;
      }
    }
    if (info.methodType === "stream") {
      rec.stream_id = info.streamId ?? "00000000000000000000000000000000";
    }
    if (info.cancelled)
      rec.cancelled = true;
    if (stats.inputBatches + stats.outputBatches + stats.inputRows + stats.outputRows + stats.inputBytes + stats.outputBytes !== 0) {
      rec.input_batches = stats.inputBatches;
      rec.output_batches = stats.outputBatches;
      rec.input_rows = stats.inputRows;
      rec.output_rows = stats.outputRows;
      rec.input_bytes = stats.inputBytes;
      rec.output_bytes = stats.outputBytes;
    }
    try {
      this.sink.write(`${JSON.stringify(rec)}
`);
    } catch {}
  }
}
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
var CANCEL_KEY = "vgi_rpc.cancel";
var LOCATION_KEY = "vgi_rpc.location";
var LOCATION_SHA256_KEY = "vgi_rpc.location.sha256";
var RPC_ERROR_HEADER = "X-VGI-RPC-Error";
var ERROR_KIND_KEY = "vgi_rpc.error_kind";

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
  const children = s.fields.map((f, i) => {
    const srcChild = a.data.children[i];
    const srcType = srcChild.type;
    const dstType = f.type;
    if (!_needsValueCast(srcType, dstType)) {
      return srcChild.clone(dstType);
    }
    if (_isNumeric(srcType) && _isNumeric(dstType)) {
      const col = a.getChildAt(i);
      const values = [];
      for (let r = 0;r < a.numRows; r++) {
        const v = col.get(r);
        values.push(typeof v === "bigint" ? Number(v) : v);
      }
      return a_vectorFromArray(values, dstType).data[0];
    }
    return srcChild.clone(dstType);
  });
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
function coerceInt64(schema2, values) {
  const result = { ...values };
  for (const f of schema2.fields) {
    const val = result[f.name];
    if (val === undefined)
      continue;
    if (!isInt(f.type) || f.type.bitWidth !== 64)
      continue;
    if (Array.isArray(val)) {
      result[f.name] = val.map((v) => typeof v === "number" ? BigInt(v) : v);
    } else if (typeof val === "number") {
      result[f.name] = BigInt(val);
    }
  }
  return result;
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
async function maybeExternalizeBatch(batch, config) {
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

// src/client/introspect.ts
import { Schema as ArrowSchema } from "@query-farm/apache-arrow";

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

// src/wire/reader.ts
import { RecordBatchReader as RecordBatchReader2 } from "@query-farm/apache-arrow";

class IpcStreamReader {
  reader;
  initialized = false;
  streamEnded = false;
  constructor(reader) {
    this.reader = reader;
  }
  static async create(input) {
    const reader = await RecordBatchReader2.from(input);
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

// src/client/ipc.ts
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
  let responseBody = new Uint8Array(await response.arrayBuffer());
  if (response.headers.get("Content-Encoding") === "zstd" && decompressFn) {
    responseBody = new Uint8Array(await decompressFn(responseBody));
  }
  const { batches } = await readResponseBatches(responseBody);
  return parseDescribeResponse(batches);
}

// src/client/stream.ts
import { Field, makeData, RecordBatch, Schema, Struct, vectorFromArray } from "@query-farm/apache-arrow";
class HttpStreamSession {
  _baseUrl;
  _prefix;
  _method;
  _stateToken;
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
    let body = new Uint8Array(await resp.arrayBuffer());
    if (resp.headers.get("Content-Encoding") === "zstd" && this._decompressFn) {
      body = new Uint8Array(await this._decompressFn(body));
    }
    return body;
  }
  async exchange(input) {
    if (this._stateToken === null) {
      throw new RpcError("ProtocolError", "Stream has finished — no state token available", "");
    }
    if (input.length === 0) {
      const zeroSchema = this._inputSchema ?? this._outputSchema;
      const emptyBatch = this._buildEmptyBatch(zeroSchema);
      const metadata2 = new Map;
      metadata2.set(STATE_KEY, this._stateToken);
      const batchWithMeta = new RecordBatch(zeroSchema, emptyBatch.data, metadata2);
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
      return new Field(key, arrowType, nullable);
    });
    const inputSchema = new Schema(fields);
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
    const metadata = new Map;
    metadata.set(STATE_KEY, this._stateToken);
    const batch = new RecordBatch(inputSchema, data, metadata);
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
      return makeData({ type: f.type, length: 0, nullCount: 0 });
    });
    const structType = new Struct(schema2.fields);
    const data = makeData({
      type: structType,
      length: 0,
      children,
      nullCount: 0
    });
    return new RecordBatch(schema2, data);
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
  async _sendContinuation(token) {
    const emptySchema = new Schema([]);
    const metadata = new Map;
    metadata.set(STATE_KEY, token);
    const structType = new Struct(emptySchema.fields);
    const data = makeData({
      type: structType,
      length: 1,
      children: [],
      nullCount: 0
    });
    const batch = new RecordBatch(emptySchema, data, metadata);
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
import { Field as Field2, Int64 as Int642, RecordBatchReader as RecordBatchReader4, Schema as Schema2 } from "@query-farm/apache-arrow";
var UPLOAD_URL_METHOD2 = "__upload_url__";
var UPLOAD_URL_PARAMS_SCHEMA2 = new Schema2([new Field2("count", new Int642, false)]);
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
  const { RecordBatch: RecordBatch2 } = await import("@query-farm/apache-arrow");
  const pointerWithMeta = new RecordBatch2(schema2, pointer.data, merged);
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
    let body = new Uint8Array(await resp.arrayBuffer());
    if (resp.headers.get("Content-Encoding") === "zstd" && decompressFn) {
      body = new Uint8Array(await decompressFn(body));
    }
    return body;
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
// src/client/pipe.ts
import {
  Field as Field3,
  makeData as makeData2,
  RecordBatch as RecordBatch2,
  RecordBatchStreamWriter as RecordBatchStreamWriter2,
  Schema as Schema3,
  Struct as Struct2,
  vectorFromArray as vectorFromArray2
} from "@query-farm/apache-arrow";
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
        return makeData2({ type: f.type, length: 0, nullCount: 0 });
      });
      const structType = new Struct2(inputSchema.fields);
      const data = makeData2({
        type: structType,
        length: 0,
        children,
        nullCount: 0
      });
      batch = new RecordBatch2(inputSchema, data);
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
        return new Field3(key, arrowType, true);
      });
      inputSchema = new Schema3(fields);
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
        return vectorFromArray2(values, f.type).data[0];
      });
      const structType = new Struct2(inputSchema.fields);
      const data = makeData2({
        type: structType,
        length: input.length,
        children,
        nullCount: 0
      });
      batch = new RecordBatch2(inputSchema, data);
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
      const tickSchema = new Schema3([]);
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, tickSchema);
      const structType = new Struct2(tickSchema.fields);
      const tickData = makeData2({
        type: structType,
        length: 0,
        children: [],
        nullCount: 0
      });
      const tickBatch = new RecordBatch2(tickSchema, tickData);
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
      const emptySchema = new Schema3([]);
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
    writable.write(bytes);
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
      const emptySchema = new Schema3([]);
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
          const emptySchema = new Schema3([]);
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

// src/http/bearer.ts
function bearerAuthenticate(options) {
  const { validate } = options;
  return async function authenticate(request) {
    const authHeader = request.headers.get("Authorization") ?? "";
    if (!authHeader.startsWith("Bearer ")) {
      throw new Error("Missing or invalid Authorization header");
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
    throw new Error("Unknown bearer token");
  }
  return bearerAuthenticate({ validate });
}
function isCredentialError(err2) {
  return err2 instanceof Error && err2.constructor === Error && err2.name !== "PermissionError";
}
function chainAuthenticate(...authenticators) {
  if (authenticators.length === 0) {
    throw new Error("chainAuthenticate requires at least one authenticator");
  }
  return async function authenticate(request) {
    let lastError = null;
    for (const authFn of authenticators) {
      try {
        return await authFn(request);
      } catch (err2) {
        if (isCredentialError(err2)) {
          lastError = err2;
          continue;
        }
        throw err2;
      }
    }
    const error = new Error("No authenticator accepted the request");
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

// src/http/handler.ts
init_zstd();

// src/wire/opaque.ts
function isOpaquePassthroughType(type) {
  return isDate(type) || isTime(type) || isTimestamp(type) || isDuration(type) || isDecimal(type) || isLargeUtf8(type) || isLargeBinary(type) || isFixedSizeBinary(type) || isDictionary(type);
}

// src/wire/request.ts
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
      if (value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
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
var TOKEN_VERSION = 4;
var AAD_PREFIX = _UTF8.encode("vgi_rpc.state.v4\x00");
function computeAad(principal) {
  if (!principal) {
    const tail2 = _UTF8.encode("\x00anonymous");
    return concatBytes2(AAD_PREFIX, tail2);
  }
  const pBytes = _UTF8.encode(principal);
  const tail = new Uint8Array(1 + pBytes.length);
  tail[0] = 1;
  tail.set(pBytes, 1);
  return concatBytes2(AAD_PREFIX, tail);
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
function packStateToken(stateBytes, schemaBytes, inputSchemaBytes, tokenKey, principal, createdAt) {
  if (tokenKey.length !== 32) {
    throw new Error("XChaCha20-Poly1305 token key must be 32 bytes");
  }
  const now = createdAt ?? Math.floor(Date.now() / 1000);
  const plaintextLen = 8 + 4 + stateBytes.length + 4 + schemaBytes.length + 4 + inputSchemaBytes.length;
  const plaintext = new Uint8Array(plaintextLen);
  const view = new DataView(plaintext.buffer);
  let offset = 0;
  writeU64LE(view, offset, BigInt(now));
  offset += 8;
  writeU32LE(view, offset, stateBytes.length);
  offset += 4;
  plaintext.set(stateBytes, offset);
  offset += stateBytes.length;
  writeU32LE(view, offset, schemaBytes.length);
  offset += 4;
  plaintext.set(schemaBytes, offset);
  offset += schemaBytes.length;
  writeU32LE(view, offset, inputSchemaBytes.length);
  offset += 4;
  plaintext.set(inputSchemaBytes, offset);
  const wire = sealBytes(plaintext, tokenKey, { aad: computeAad(principal), version: TOKEN_VERSION });
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
  if (plaintext.length < 8) {
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
  const stateLen = readU32LE(view, offset);
  offset += 4;
  if (offset + stateLen > plaintext.length) {
    throw new Error("State token truncated (state)");
  }
  const stateBytes = copyAligned(offset, stateLen);
  offset += stateLen;
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
  return { stateBytes, schemaBytes, inputSchemaBytes, createdAt };
}

// src/http/dispatch.ts
async function deserializeSchema3(bytes) {
  return deserializeSchema(bytes);
}
var EMPTY_SCHEMA = schema([]);
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
      resultBatch = await maybeExternalizeBatch(resultBatch, ctx.externalLocation);
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
    return produceStreamResponse(method, state, resolvedOutputSchema, resolvedInputSchema, ctx, parsed.requestId, headerBytes, reqBatch.metadata ?? undefined);
  } else {
    const stateBytes = ctx.stateSerializer.serialize(state);
    const schemaBytes = serializeSchema2(resolvedOutputSchema);
    const inputSchemaBytes = serializeSchema2(resolvedInputSchema);
    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, ctx.tokenKey, ctx.authContext?.principal);
    const tokenMeta = new Map;
    tokenMeta.set(STATE_KEY, token);
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
  let unpacked;
  try {
    unpacked = unpackStateToken(tokenBase64, ctx.tokenKey, ctx.tokenTtl, ctx.authContext?.principal);
  } catch (error) {
    throw new HttpRpcError(`Invalid state token: ${error.message}`, 400);
  }
  let state;
  try {
    state = ctx.stateSerializer.deserialize(unpacked.stateBytes);
  } catch (error) {
    console.error(`[httpDispatchStreamExchange] state deserialize error:`, error.message);
    throw new HttpRpcError(`State deserialization failed: ${error.message}`, 500);
  }
  let outputSchema;
  if (unpacked.schemaBytes.length > 0) {
    outputSchema = await deserializeSchema3(unpacked.schemaBytes);
  } else {
    outputSchema = state?.__outputSchema ?? method.outputSchema;
  }
  let inputSchema;
  if (unpacked.inputSchemaBytes.length > 0) {
    inputSchema = await deserializeSchema3(unpacked.inputSchemaBytes);
  } else {
    inputSchema = state?.__inputSchema ?? method.inputSchema ?? EMPTY_SCHEMA;
  }
  const effectiveProducer = state?.__isProducer ?? isProducer;
  if (process.env.VGI_DISPATCH_DEBUG)
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
    return produceStreamResponse(method, state, outputSchema, inputSchema, ctx, null, null, reqBatch.metadata ?? undefined);
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
      if (process.env.VGI_DISPATCH_DEBUG)
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
      const schemaBytes = serializeSchema2(outputSchema);
      const inputSchemaBytes = serializeSchema2(inputSchema);
      const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, ctx.tokenKey, ctx.authContext?.principal);
      for (const emitted of out.batches) {
        const batch = emitted.batch;
        if (batch.numRows > 0) {
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
async function produceStreamResponse(method, state, outputSchema, inputSchema, ctx, requestId, headerBytes, requestMetadata) {
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
      if (process.env.VGI_DISPATCH_DEBUG)
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
          batch = await maybeExternalizeBatch(batch, ctx.externalLocation);
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
      const schemaBytes = serializeSchema2(outputSchema);
      const inputSchemaBytes = serializeSchema2(inputSchema);
      const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, ctx.tokenKey, ctx.authContext?.principal);
      const tokenMeta = new Map;
      tokenMeta.set(STATE_KEY, token);
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

// src/http/landing-html.ts
var LANDING_HTML_B64 = "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImVuIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgbmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCwgaW5pdGlhbC1zY2FsZT0xIj4KPHRpdGxlPlZHSSBXb3JrZXI8L3RpdGxlPgo8bWV0YSBuYW1lPSJ2Z2ktbGFuZGluZy12ZXJzaW9uIiBjb250ZW50PSIxIj4KPGxpbmsgcmVsPSJpY29uIiBocmVmPSJkYXRhOmltYWdlL3BuZztiYXNlNjQsaVZCT1J3MEtHZ29BQUFBTlNVaEVVZ0FBQVFBQUFBRUFDQVlBQUFCY2NxaG1BQUFBd1hwVVdIUlNZWGNnY0hKdlptbHNaU0IwZVhCbElHVjRhV1lBQUhqYWJWRGJFY01nRFB2M0ZCMEJQeUJtSE5La2Q5Mmc0MWZFcEJmYTZnNVpZQkMyYVg4OUgzVHJFRGF5dkhpcHBTVEFxbFZwRUo0QzdXQk9kdkFKSGp5ZDAwY0tvaUpxSkx5TVYrYzVUemFKRzFTK0dQbDlKTlk1VVMyaStKZVJSTkJlVWRmYk1LckRTQ1VTUEF4YXRKVks5ZVhhd3JxbkdSNkxPcG5QWmYvc0YweHZ5L2hIUlhabFRXQlZqd0swcjBMYUlESVlHMXprUTZNWGNGWWVaaGpJdnptZG9EZlhPVmtOYmFFS2NRQUFBWVZwUTBOUVNVTkRJSEJ5YjJacGJHVUFBSGljZlpHL1M4TkFITVZmVTRzaUZVRTdTRkhJVUIzRUxpcmlXS3RRaEFxbFZtalZ3ZVRTWDlDa0lVbHhjUlJjQ3c3K1dLdzZ1RGpyNnVBcUNJSS9RUHdEeEVuUlJVcjhYbEpvRWVQQmNSL2UzWHZjdlFPRVJvV3BabGNNVURYTFNDZmlZamEzS25hL0lvQXdCakNPRVltWitsd3FsWVRuK0xxSGo2OTNVWjdsZmU3UDBhZmtUUWI0Uk9JWTB3MkxlSU40WnRQU09lOFRoMWhKVW9qUGlTY011aUR4STlkbGw5ODRGeDBXZUdiSXlLVG5pVVBFWXJHRDVRNW1KVU1sbmlhT0tLcEcrVUxXWllYekZtZTFVbU90ZS9JWEJ2UGF5akxYYVE0amdVVXNJUVVSTW1vb293SUxVVm8xVWt5a2FUL3U0UTg3L2hTNVpIS1Z3Y2l4Z0NwVVNJNGYvQTkrZDJzV3BpYmRwR0FjQ0x6WTlzY28wTDBMTk91Mi9YMXMyODBUd1A4TVhHbHRmN1VCekg2U1htOXJrU09nZnh1NHVHNXI4aDV3dVFNTVBlbVNJVG1TbjZaUUtBRHZaL1JOT1dEd0Z1aGRjM3RyN2VQMEFjaFFWOGtiNE9BUUdDdFM5cnJIdTNzNmUvdjNUS3UvSDg2eGNzdFYwdjNNQUFBTmVtbFVXSFJZVFV3NlkyOXRMbUZrYjJKbExuaHRjQUFBQUFBQVBEOTRjR0ZqYTJWMElHSmxaMmx1UFNMdnU3OGlJR2xrUFNKWE5VMHdUWEJEWldocFNIcHlaVk42VGxSamVtdGpPV1FpUHo0S1BIZzZlRzF3YldWMFlTQjRiV3h1Y3pwNFBTSmhaRzlpWlRwdWN6cHRaWFJoTHlJZ2VEcDRiWEIwYXowaVdFMVFJRU52Y21VZ05DNDBMakF0UlhocGRqSWlQZ29nUEhKa1pqcFNSRVlnZUcxc2JuTTZjbVJtUFNKb2RIUndPaTh2ZDNkM0xuY3pMbTl5Wnk4eE9UazVMekF5THpJeUxYSmtaaTF6ZVc1MFlYZ3Ribk1qSWo0S0lDQThjbVJtT2tSbGMyTnlhWEIwYVc5dUlISmtaanBoWW05MWREMGlJZ29nSUNBZ2VHMXNibk02ZUcxd1RVMDlJbWgwZEhBNkx5OXVjeTVoWkc5aVpTNWpiMjB2ZUdGd0x6RXVNQzl0YlM4aUNpQWdJQ0I0Yld4dWN6cHpkRVYyZEQwaWFIUjBjRG92TDI1ekxtRmtiMkpsTG1OdmJTOTRZWEF2TVM0d0wzTlVlWEJsTDFKbGMyOTFjbU5sUlhabGJuUWpJZ29nSUNBZ2VHMXNibk02WkdNOUltaDBkSEE2THk5d2RYSnNMbTl5Wnk5a1l5OWxiR1Z0Wlc1MGN5OHhMakV2SWdvZ0lDQWdlRzFzYm5NNlIwbE5VRDBpYUhSMGNEb3ZMM2QzZHk1bmFXMXdMbTl5Wnk5NGJYQXZJZ29nSUNBZ2VHMXNibk02ZEdsbVpqMGlhSFIwY0RvdkwyNXpMbUZrYjJKbExtTnZiUzkwYVdabUx6RXVNQzhpQ2lBZ0lDQjRiV3h1Y3pwNGJYQTlJbWgwZEhBNkx5OXVjeTVoWkc5aVpTNWpiMjB2ZUdGd0x6RXVNQzhpQ2lBZ0lIaHRjRTFOT2tSdlkzVnRaVzUwU1VROUltZHBiWEE2Wkc5amFXUTZaMmx0Y0RvMllXWXpNekEyT1MweE9XSXhMVFExTXpVdFlqbGtZaTB6TWpka01EQXdNVFE0TldRaUNpQWdJSGh0Y0UxTk9rbHVjM1JoYm1ObFNVUTlJbmh0Y0M1cGFXUTZObUkwTlRnMk1EZ3RaV05sTWkwME5UWXdMV0k1T1RRdFpEZzFPR1ppWldFM1pEVXlJZ29nSUNCNGJYQk5UVHBQY21sbmFXNWhiRVJ2WTNWdFpXNTBTVVE5SW5odGNDNWthV1E2WVdVek1qSXlZV1V0TnpSbFpDMDBNV1JrTFdKbU9HRXRNalpqT0RnMU1HVTNObUkwSWdvZ0lDQmtZenBHYjNKdFlYUTlJbWx0WVdkbEwzQnVaeUlLSUNBZ1IwbE5VRHBCVUVrOUlqSXVNQ0lLSUNBZ1IwbE5VRHBRYkdGMFptOXliVDBpVFdGaklFOVRJZ29nSUNCSFNVMVFPbFJwYldWVGRHRnRjRDBpTVRjM09UZ3lNakExTWpVd05EY3pOQ0lLSUNBZ1IwbE5VRHBXWlhKemFXOXVQU0l5TGpFd0xqTTRJZ29nSUNCMGFXWm1Pazl5YVdWdWRHRjBhVzl1UFNJeElnb2dJQ0I0YlhBNlEzSmxZWFJ2Y2xSdmIydzlJa2RKVFZBZ01pNHhNQ0lLSUNBZ2VHMXdPazFsZEdGa1lYUmhSR0YwWlQwaU1qQXlOam93TlRveU5sUXhOVG93TURvMU1TMHdORG93TUNJS0lDQWdlRzF3T2sxdlpHbG1lVVJoZEdVOUlqSXdNalk2TURVNk1qWlVNVFU2TURBNk5URXRNRFE2TURBaVBnb2dJQ0E4ZUcxd1RVMDZTR2x6ZEc5eWVUNEtJQ0FnSUR4eVpHWTZVMlZ4UGdvZ0lDQWdJRHh5WkdZNmJHa0tJQ0FnSUNBZ2MzUkZkblE2WVdOMGFXOXVQU0p6WVhabFpDSUtJQ0FnSUNBZ2MzUkZkblE2WTJoaGJtZGxaRDBpTHlJS0lDQWdJQ0FnYzNSRmRuUTZhVzV6ZEdGdVkyVkpSRDBpZUcxd0xtbHBaRG93TmpGaU5HTTVNQzB6TURNNUxUUTFOemd0WVRCaFl5MDBNV1E0WlRnd1lUVTRZMllpQ2lBZ0lDQWdJSE4wUlhaME9uTnZablIzWVhKbFFXZGxiblE5SWtkcGJYQWdNaTR4TUNBb1RXRmpJRTlUS1NJS0lDQWdJQ0FnYzNSRmRuUTZkMmhsYmowaU1qQXlOaTB3TlMweU5sUXhOVG93TURvMU1pMHdORG93TUNJdlBnb2dJQ0FnUEM5eVpHWTZVMlZ4UGdvZ0lDQThMM2h0Y0UxTk9raHBjM1J2Y25rK0NpQWdQQzl5WkdZNlJHVnpZM0pwY0hScGIyNCtDaUE4TDNKa1pqcFNSRVkrQ2p3dmVEcDRiWEJ0WlhSaFBnb2dJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdDaUFnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FLSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUFvZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0NpQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQUtJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQW9nSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnQ2lBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBS0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lBb2dJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdDaUFnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FLSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUFvZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0NpQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQUtJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQW9nSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnQ2lBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBS0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lBb2dJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdDaUFnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FLSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnQ2p3L2VIQmhZMnRsZENCbGJtUTlJbmNpUHo3Y0FaQXRBQUFBQm1KTFIwUUFBQUFBQUFENVE3dC9BQUFBQ1hCSVdYTUFBQXNUQUFBTEV3RUFtcHdZQUFBQUIzUkpUVVVINmdVYUV3QTBzNXREVXdBQUlBQkpSRUZVZU5xMHZXbXdaZGQxSHZhdHRjKzk5NzErM2EvNzlUdzN1Z0gwQUtBaEFDUUlVQVRCUWFSQWloTGpRSU1sMmJJcEs2WElTVXBPSlZGVktuRXFTbFhLcWtxNU1wVEtrVjJ5TER1eUpVc1JKVkV5SlpIaVBBa3pNUkFEMFJpNmdRYlE4L2pHZTg5Wkt6LzJ0UFk1OTc1K1VKSm1nZDM5K3IxNzd6bG43N1hYK3I1dmZZdisxNmZPS2NQL1VsVUFBQUdBeGorWVg1TytGbjlSL2pzQlVDSkF0ZnN6NW51SjhqLzY5L2Yva0Y0cXZoNFJGT3AvQmdRbEhmLytZMzRwQWY3YnFmdkJ3MXNxRkJTL1VRa1VQb0RTS3RmZXVtZ0Z3SzNQN2I4amZQYjJUMUw1ZGZ0OUNnVk51cUJ4ejBSYjk1OHc1aDB4L2pPb1RuekdwUDR6S2VXWFovakg2bS9xR2o1amZObHh6MHJIL0gwdDY2NTFEV0VCQVVyNWFYUmVjeTF2UmhoLzUvTFBhcmd2NlQyMDlhTTBidTFOZUw5aXYrU3Z4YTJUdjMzdDkzcjhmYVBXaHZKL3JOaDhqMStRR0x0cHV3c3FiQlRORjJwZldNM1ZwZnZTdmlxYTlDQXBiTWp3Q21HRDVuZlc3ak5hWlhPV0Q4RGVFRFUzbk13MlRHdXB1SCtUN3JMbVpaRVd1b1lmSU5Md1l1VTlwTFRKVGFBcEFpaU5XVUIyWTVvcjB2eDg3VjVZVS94UVJYR2oyNDg0TG5KU2lCS1k0cy9RS205UWJxTDQzSW55czAyTGs4WThtdlpucDBtUE9VWWx6UUdVWXVDazRuN2tIellIRGlqRnNQemN0QnRZNHdhSTYxblIzZndUMWdscENCYmMzb2lyWEZ1NEppSkNqRE5RQW9YSXEydmMrUFlnTDRLOCtjWFVQcTdDdFpJNWZ3Z0VTRHRxK1hkUVFycTRJbk9nN3ZVS0pwMUsxRG14NHdJalFzb0l5RDVESXBSZnhDcFJlOXlYdGZ2alpLSWpkVjlhSnoza1lzdmFHNkhRZUNLeDJlVHh2cGxvMERtSld4OEorU1h6ZlcxZEFIV2U0VnBQaXZKRml1c3d3WmNoSUNpSUNYQUVGQm1NcHY4VnA0ekpMRFc4T0pFSnVEUjU4NnkydHlpZCtCcGVpOUp4UTYxZ1dDNmNjVXRCVTBCR08zc29GMGczNmJuaEdyUnBJWFYraHNaZHNEbVF5QVJnWGNQbXQyK2pZWTFvM2s3aDNzZC9WMVJLeGRKTml6bmVNR3JkR3J0ZVlocVVrbFkxSzN0TUdrZlFJdVVmOTZoVDRBMVppQ3FLaHhzM3JxUXo5bS82eXgrdEtaaWJrOEJtdDJPRHRZNDU1OVF1K1BUeTZRUTFCMVVuT0tiRnZJYVAzTjVmMnIyazlOQmpHVEUyR2RBeDBZM0tPQk1PSHJBcXBtUUlMRjVIdmJRSTZ2ZFJyWi9Ec2h0QWljTFpRS0NVVFZBM2VKbXNVVW5ISENiYXVjbXJsVUJxVG5RMU41YUtBRS9GOHlsTHJ2Q3ZSRDdoVlYxVEpOSUo1OHFraENzOTIzSHJ2cFhJcGd5eGVINzVzK2thbGdhWjF5VmJwWXpKREFsQVJUZXFMY0pHWkNLSVRabmpRektaTklXbnJtUHVIekdOVFdVbjV3S3RFOUhzb0hIM2MrMFZVbHhzMmxta2FuYUFycUV5MUJza0hXcExJZ29uazJxUm9ta1orRlBBWGUzamE3ZzNTbTJzSUc4a2l0OW5xaDFlclF3MmJ5dnFUM2wyREc1R29DdG44ZmhmL1JrMjZTSnVPWFFUcmkwczRaWHoxM0huaHorQjZXMjcwQkNoRmtWTkRuQTlxQWkwVVhEcmNFbUhoUVVVTUs3ZWJhK0V5U1dHWFgrMklJdUhpQktsdzE4N2RZYlBTMzJKc3NwOXAxVVdtTGF6SmdzYzJXYzZwcXhBdHlMVm1OSEVZQzdtc0YxbEF4bjRMRmNyN2ZMY0xvWndVNnExN2toTmdCSEEzVndzWFNSTkFHa3l3S1UzS3FvbmdsaVRnQzFhODlIWkJuTldBU2pYVmt4MGY0Ykt6NXNmZ0pZbmZndzRxdVdERzN1OFVBbGl4Y3lGQUNhR3F1U25YdFMwWmFRY3U0YXB1d2lKZlo3SVVvTXZ2NDFIL3VDMzhROSs1bUZzMjc0VkY4K2Z4N3FaRGZqRTFCVCszZS8vSWFyMUd6RVVoZlNuc2VXbXc5aTQvMWFNcW1tL1NMVEVBTFNWOHhKMTA5YTFvMXZsSllncFhlTUpzWlpEUVNjZDZSMDBsRHJBOGRoa1FXbkNQZlkxdkJiUFJMdVBnQTBPTis2anJKSnEyQ29uWnpzbWg2Y3V2a2YveDFQbjlGM2RkeHBmajA2T25sU0FnQnBEMmpnRTI0SVc0Z0UwMVRMTXByUldKNVVURXhEeXRjYUhWYUs5amttMWJnZ2V0KzhEbGFXRVRtSko0cFhxR01pUmN1bmxTREdRSVpxVkpValR3RG1HcTNvUTEwZE5EZzA1Z0Rna1BXcXlISjE0Q2drRHBJS1plaDVmL1RlL2dZYy8va0UwSU13dkx1QUxmLzRYV0Y2Y3gyZCsvalBZc25rTzI3ZHZ4Y3owT3Fnb25uditKWHpsMlJPNDlVTS9pcVgrTklnY1hNcG5WenRkL2VsRzVqaFhVejZ2NVZFV2V6TGt2NTJLbWFqRHNOZ3NNeVZEYWpCaUU1eFhmKzhjZ0ltNlpUT05BNDUwZkxhOWxzT0hicENCS2xwQTRDVDhhbUlBdUZITzJ5cHEwOUt5c0RZUlNyYW9SWWlsK2w0TUF6QUdnVjVyVlo5T21qSHNEOTBva2s2bUxDMUExOG1ZeDlYUzBOYnhyd25vdS9ISk00RWVNOHlFUWdBSVdCV2JhSVJudi9BbmVPSnJYOFROKy9kZ3k1Yk5XQm8xV0VJUDJ3OGR4ZjdiN3dZMmJzV0lxbEFHbU5KcVVxQWpvSklWWFA3ZVk4RGJKN0QvME0wWXFxQVJ3V2hsQmZWd0JiT3pHOER3QjczVUl3eVhsMUZWUGJ6NitodFkyTFFQdDM3b2t4aHhENndLQ0pXMEJZMWhhdFljbzhjdi9YZlBIdVlBa01GQ21semtyL0o0ZEpXS1FRMmRXa1N6Y1ZtUTZwb3p6ODV5YnBFU1dtQ3NrN1B1YWlKdmpqS2xIUHV1Sm1MN2RKTnpnVTQwaG5uUkxwVklrV3ZHR0JSOWJRZDQvQ1dxWTBIbFREK1Y3d3NGbEZ1MHpwaVZvNnM5R0JxWGlsRStQY3dSSHJNV05iQ20zcWowTXZkWXdtMTEybUJRTCtQQ2E5L0hTODg5aVd1blg4Zk8yZlVZTGkxZ3o0NmpPSGp6UWN6TnplR2RkODdncTMvNEw3SG52UTlpNS9IN3NGUk5RUnBOWVpoYXo1TUFTUGk4VTdLQzczNzlTL2praHorQXhaVmxnQjBJQVBjcVREbUhlalRDaGJObjhkenpMMkNGcDdENzRHRnMyYlVINjIrN0g3UFRHM3pXRWM5R1dxM2tLazlnSHB2Z2R5RHlpWnVGeGhCUlpSQ1BuMHJDWndTSU9QeWJyaW1kMHpWc1VKMVVhclZ3RHgzM0dWc0gyVVFRc2JVTzdjSEtCZzlaTFgwWm13RWtQUXpHNnhCVWJjblppdDZVb2VpeE1vc0F6S1FhMVR3eUN3aVRJWllteFhGN01wZGxSa2p4Y2lLQ3RJTWdCVWlwcXgxT055d3Z0RldqVDhnMlVxU25MZ2UveWxrbkFjaGp6ZmpCUUd1c3ZQTWF2dklIL3hmMno2M0QwV1BIME84TjBOUWpYTHB5QlcrZk9RT1JCcHMyenVLKzk3NEhCL2J1eFZlKytqVmNtdHFPdmUvN0VGYXFLWkRFeEVRVHJrTkVJRkl3TXlBMUZrNDhnOU9QL0JWdU8zNG5odEtnQ2dGQW9DQVJEQmV1NDV2ZmZSNFAvdFJuc0c3YkhzRDFVSVBRS0VORE1HYWk4dlMvRVZOR3VYNlBFZFN1czdHVnFLNGRHeXF5Q0ZJb01VaHpTWmtCUERYQXdnMHlBRXZmVGdoTUtETGdjbjJrOWFmYUNUdTZXaXhhYmJIcUdKWUo3eUlBM1BERkk3cEtrOU9MbElKb1RxMnBSVVA0Rzl5U3JyVWtnSHFqbEwrem9hZ2JMSFZNRGExcmh6NG0zaDZkbE8rTkJ6VXpGVlhVQVMwdFlCUTYrWHNzc1hSUVFSODFycjM4TEw3Kys3K0ZqM3pvZ3hpdURISGl4S3RZV1ZrQ2c4R1ZnK3YxdldDRVBDMTMrNUZiOGJFUFA0Zy8vdHgvd1ByYlA0RCtUY2VncnRlcFZPSm5aRktzYTFidzlkLzVQL0hlb3dmUkVQdWdFTzZyUURFN05jQzV0OTVDZGZOZDRBTzNnNm8rT09BVmFtNHRwNHpNRkg1R2RVbHJnSVFMY1ZxNFEyS3lGNTBrcmlOVGVyYkVWV1dzOXZjWkU1K25qdFVseVNycGZydWtKSk9UeFBjckQxZnRuUDVycXYxWG9YTFhvcDVVMVRXd0FHT1IrTUNocXIxT0xZcWFRbjAzQWUwbGJsVi9aRG5MVmNBcXN6Q29EYlJNMkh3cFUyaW5WNnMreEFsZ251cDQ1UDhHYkNwYXN1ZU8rSWRLU1I4bFlNeURjblQxSEw3NmU3K0YyNDdjakNlZWZBcjFhSVNsK1VVc0x5MUNGWmlhbXNiMHpBeW0xazJqMSs4QlJIajJoZTlEcGNGREgvOG9mdlBmZnc3Mzd6dUlwYXJuTnlCSnNZRllGUU1STEx6eENxYnFKVlQ5UHVyaENMVUlXRE9LdlhsdUkwNmRPSUhkbTdaaXBScWdhVDNick5HaTd1bXRSdVc0Q3VTUm1DelZNZERvRFpuU0x1bFRJT0hoZWszcW9HTVhnSTU5ckRkYUs0U1M5V21yRDIzZ0pYdDRyZUZnV3RQbVh3T3JFdThyanoxTmFYS3hreDYwcmZPVGdJRTZyN1VhVWorMnR0WVNJYUcwbWlnRGFkUmRMQlBmUjgxblN5Z3d0YTZYVXIxTmxPVlNSTlI5WFpVMW9UUmtNQkQ3bWJXbExTN3VWWWltRVpOUXpacjdLYTd4N2M5L0ZodW1Lanovd2d0b1JMRjMzMzRNUnlQTWJkMkszWHYzWW03TFpneW1wMEFFakVZampJWkRqRVpEUFBIVXMzanpyWGR3ejlFRHVIVHlGVGdWTUFrWUNnNGJ5a21ONldZSjExNTdEbC81ZzMrTjIyOC9odVdWRlRTcUVCVTAwcUNwR3pSTkE4Y09XN2JNb2FtSFh0eGxONHNDSExOcDdhSStiTzdwdUhXaVk3UVI3VC9INTZrVHM2M0pKYU1WQjRsR2RXcE0rWFhzUTAwWmdsa2JkcTJRbGRlTktXM2lmYUdrUlNqVms2bHNMc3Jvc1A1dVJEdnI2cWM5amJuZlpOWjIxZDE0UmlsbHFSQmFEUjZoUXM1YVVDdXJSYlF4TjgybXlBcWo4VGEwSHhObER0MGlxR3hFR0RkS3g4M0dWSk9hdDJtaGNYZStDMWhPRG00bHV1c2Z0c1RVczdXQ1MyclRVSi9hWVBuOFd6ang2RGV4Zm00elB2MHpQNCtaUVE5Lzh1OS9CenYzN3NiUzRqSVdscFlCRlRnT0tUdXpYMGZpT1lPdmZmMmIrTkZQUFlUUGZ2bWIrTUNSTzdDRUh0aDUwTFluUXl5ODh6cSs5QmQvakUwMHhJODk5RVBZdVdzWCtsTlR1SHoxT2s2OStRWWF5WExKaGNVbEhOaTNIeTllUEkrWjdRY0E3Z0hpcjRRN3NONk5nLzVhUURSOUY2OHo4ZCswMU1MWU1wTVNHNkNkekxPQWoyMG0yU3JseGk4SDA5eFdnTlE1ajFBWUpjME5Tb0JPM3JyR2s3NVlrK1pyMVVReW1DYjBoM1NsVngzTjkvK2JYMFZxYmpHWnRWQm9TcFA1VHNOdWpPTis4L2ZjT0EyakRoQzRSZzFpRkZJRitGZmJRQUtWc0M2RlBWZXA0UFFySjdEdnRydnc4Qy84RWxDdjRGLzkycTlpMjVZdFdKeGZ4R2g1QmN1TGkxQVJVRldoMTZ2UTYvZFJWVDFQK3pGdzd1SmxuRDkvRVhNOHd2RGlXZkRXdlNCcHNHNjBpR2UrK2hkWU9QVThmdnlqSDhWZzNYb3NyYXpnMnZYcm1LNEZCL2J2eDl6c1JqejIxSk1nNXdBVlhMdDJIZnNQSDhMWHYvRTBiamx5RCtxV0JEbWw2aFA2RzFaYm5QOS8vdEtBYkpOMjExbm56TEFiVVRPQVRlMU5UOVE1RnN0U1BBTzZPWEFZZlF1MXNKajI1cjhSNGY5dXJuK01kcVphOVdSdXlSb1RZRkY4UUNyUzNmOVBIbVRxSGlzN0VUU2UvaEdFVk8xSXlTZlZSSnJrQ1ZyUWRWRXdxd1lnV3ZNVkVFcTkrWmg0eEcwc1F5ZXgyQ2hVZkVnYktxRFZNM1A0c2YvMHYwSXpxUENsZi8wYm1KMmV3c0wxNjFoWVdNQzY5UnNnVW9PckN0TlRVeW4vSHRVak1ERklDYTZxOE9oamorUGU5OXlOWi83NmE3ajNSMzRjVjg2L2c4LzkvbS9qdnRzUDQrREhQNGFUYjd5Tks5ZXZBY1RvVlQyNFhvVVhYem1CZTQ0ZngvVnJWekc3Y1E2QVltbDVHWVBCQUZmT253V0pnQ29rb1pPOWVDR2RxQXdkdC9IYkFlUGRQSXZFL2t4NDdYRmMrV29CWGxJUFIwdWMzYXJWTXhHVXNTRzZVZFp1dGJsdE1VbDdMK203MlRaVVpDaGpzNVZWZFFBZFlJSFdscDRsZmxYWENKK3ZMUTlvUDBDMkxablFMdUJUZ0k3VWppY2xEMXVVRU9QYSsvd041QnN0bUJpc3hud2ZHWnBVZ1c0LzlzUkx0elFnTUFKaDl3KzhENWRGTUxoMEFTOC84UWgyYk42TVMxZXU0c0dIZnhwMzMza2N2L0ZQL2tjTStnT01wUGFNQUhsNXNEaUFHZ0pRNDh5NWk3aHc0UktHWjk3RVgzLzIzK0ROVjE3R0xZY080TksxZVZ4NCtqazQ1MEFnY09WTEQxSEZxSzV4N3R4WmpJWkR2NmhFMGRRTnBGRklQVEliU3p1Ni83RnR6TFkxdFpVOWRRRy95WUcxTEptcFRHOFREWDBEVDRVSjYxR0ttS3hGa3hWUU5nNmx3MGt4VVRGWVFHcmgwR0pRWWt5c24wRnVteDlmeGE1SzBJMUw5d21yWnJRTXJLWG4vVzlTdGEyQm1KMzB0UW5vanFpYVRyc1NDY3lvYzdleFl5dzF1MHBySmFXT2NpcisvcTdxemJGM1ppMmxSVnR4U0NCaTFBQkVCV2RQdm9vK0E1ZXVYTUhIZi82WGNOUDlIOFdqVHowTkJqQmNHYUllMVdqcUduVlRRNlNCTk9HL3VvR0s0cThmZXhMT01TNmNlaFg3OXV5R0tLTnVGQ3FBaUNRQmo4ZHlHcEFDWjg2Y1E3L1hMektWNWVVVktQZEE3SEtsckxvS2R0NTlGcVEwbWFwZDdUN3BEWjZCMGxoV2lHNjQ4TEE2ZHFBRkU5NHRXVzBKMUtxU08rQW1sVUZHTlI4Vi9zL2FZVVRXTEl6VnlmZHFiQUI0OTBrN0ZVaGltV2JRUkNLaDdMMGRVNHdWWFcyU085c1NqR3BGUDFyVW5CTFR0bFVDeUxnVWREWDJnTWlnemFBdXNtOVJWdW91cjFqQ2ROdkZkUXkxWWdCZElvK1cyMm9NaEI0VHJwdy9pNlp1Y1B6RG44QzJZL2RpU1FpdnZmUWlJSXFWcFFVc1hMME1FWUdJb0s1ck5LTWFUVk9qcmtkbzZocWo0UWdYTGwzRDNOYnRBQkVhRlRSTkE5V21nRGg5T2wyQlhZVm5uMzBXVTlQVFVQR0xzMWRWT0h2MkxHYTM3WUN3UzZ5RmtBRTlTY2VpenhqREluVzUrZkhJTFpuT1ByU2VZZnUvR3luZzFyS2IycStwTGFhc1lDZTBGZHpXc2xPNS9ibTlLdEhDanJRR3FLbXpobWs4Q3pBaEFPaGtoeDJpTVcyVXlGSEtJUDRkRm9FOHdVSmt0bWphMUswZ1FES0c4RUdTWnlyVW4wN2lGNlpvQTRINHhpSVZLT0ovNkxTMmpxdGtiZ2crR2VVVzBPcXlHa3QzS2xwTVkwanZ0QlA1N1pPa1FCSVJqYzlPeVB5dy80cEFwVUhUbjhMeGozd1NpNmpnd05DNnhtaTRoRzI3ZDZNL05lMVRTVkdJcUtmdm1nYlNDT3FteHZUNkdjeHQzUXBpOWtJakZYOS93L3NJZkNiUXFBSk5qUXRuenFBZWprRGtJSTEvcmRuWkdienc0b3ZZZC9nMk5NVFpuS1FWemRwclF5Mm9GdFpPNFh5RVNTM2psTDVmVzBld1paNVVNM1dxcXA3VzA3OTVlbHUrYm51SGF6ZG5wTFVudnFvdEF3YnpmbVA5UldqdHpJcFM5eG9tc1ZtY3VjWnhiS3ZralJ2SkNocm5CalhHYVVWYjZoZ2FjOG9YZEZjTUNwSmJqQU41bWd4SVdNRU1FRGtRc3dlNDJBdFVpTDJ3QlNGOVVwSll6U1dqQ2lxMzRocVVGbVBvR0JwM2xwTloyRzByR1VyL2kya3ZkZnkvVEtZUVR4dmp3eGROSVZRRVUxUHI4SUZQL2poa1pzN2ZHMlpzM3JVSEM0c3IrTEdmK3dWczNMMFAwdFJvcFBFbGdJZ3ZBYlJCNVNwczNiWWQ3TEs5RkdrTXFvcW04Vm1EcUVEcUdwZlBYOENUano2Ry9RY09vQkgvbXMxb2hFMGJadkhkRjE3RjF2MDMrMmVoQ3FjNXJBblJ4SUtuNjdLa1hZN2FGbUZFbGlUckpIZmFPYTB0bFc1Vm9XWExPdDNReG1kc1hwcURsTmp5TU93TnBYQ0N0NDlocEN5eDBJRm82ZXRtbjMzTVl1SmFvWGRSdXRBWVg4SkoxOFhoRUUwY2pvWkdsdkxyWEp6S3VmTlBjN1JWbUFpc1NhYXBFZ00yK2Y4RVVERy9hM2cvOC80SXAxTDAvbUVJWE56OFVEZ0luQXFJeEZNekNrQWxBQm9TSG9nWWt5ci9nTVNjRHY0LzhlWVZ4aGV2T0Vra1IvNTIxaUNhWDFjTCtrYVRuaURTZkxIR2l6aUdRRE5pYkFVd0ZpQXpwNTJHL3I5YUdkTTc5MlBYbmUvRlVCbU9DQ013YnJuN2ZaalpkUk42T3cvaHlMMFBZRmdQb2MwSUd1cC9iUVFxaWgyN2RtRmhjUUgxcUFFSGllL3k4aEpXVmxiUWlLSnBGREpxb0hXRGQwNmZ4aVBmK2pZT0h6MEsxKytocm12VVRZUHBRUTl2blg0THQ5Ny9JRWFEYVloS3lsVGlkWE9MWmxYS2RXMDZTVXZyRy8vem9zVXpJOUlXcm1CQjMxSWliSitieFNSc2owL0t5TlFLY25Rc1VLUXQzQ0t0NlZBYkp1RFJ2S2UvVGtsQklUMWJBU1FzdnJ6L0phOHZsT3NTQVd6MXN1RUpHVHB1VUc2WVhVL204K2YvZkVDc0Npb3owbUwyQWtuTWlVWEZoOG1OZndRUjAxMFdUblJTeW9hRUZBd3l3VGx0US9DTlVNc2RNMEJlZXVwUEFPUGRSd0p5QUN1SHBKOUM2azlnMjNCQkJNQ2xiQ0IrSnVLaXFpb2dWZDhsNTRzaVRvMUpkaUhIYXc4Um1pbWRualRHT3N3ajFWclV2ZU9hdWlRWXJCUjlNTkNBRWdmZGU5U1F1d3FiRGgzMVhna2M2Q25IMkxqL01EN3drNS9CRlV6aDJBOStCQ2VlZVJJWFgzMEJIQUpNVFl6WjJWa3NENGVZM2JnSmk0dEx1SHp4RWx5dnd1eW1qZWozK3BpL09vK21Ic0lSNGRUSjE3QzB1SWhEaDIvRnBxMmJVVGNOV0gwWnRtbkxIUDc4SzkvQ3AzLzV2OGNTcXNKZEpxNFFDZUJTdHFSaWI5WkpYV1ErWlRta1JUK0laaXNjSUppUkNySW1KRXFLRTJaUm1HQnFXZ2NXV0tQd2JNVmtIUlozc2QwM1JlK0craVlwLzN3MDBZMGNjQVp1TVdFS0xXZ25ZazRaaUZvUFE5Ymt1a3dHZkJYTFZ4SjV0U2JuUFJYVm9XcHA0dzVsYjBSMFVIQXMxVFJMQnhXYW00RW10a0tiQjJ4NzlPT05qTW1haE1qbm94ZkE3T3ZMRW9BSURaOVVOb2trL0ROYVowRkNyN20vS1JWN3hacWxBU1BvWkUvMVJrMFFVWE1DRk8zRkREVkJMUm1NU09uVXduWlRTeVFYc3VkY0trOUVVcm5EMU1MeE5hZC9IUThZOVgzOS9qVno5MkxSVzJFMEQ0Z29SN0N2U2dycEdNMEphQlRva1dMZGNBRlBmK1hQOE5RWC94VE44aEkyYk5tR2ZUY2RCQkhoK3ZYcklHTE16VzN5L1FJS0xDN080OXpaYzdoNDVoeVdGaGV3WVhZamR1L2ZpNjA3ZG9DZFM0Qmtqd2x2bm5vRHQzL3FwekIzNUc2TXdCbkoxMHpQQ1JrS01Mb1dTYzZHSW83U01ZY0pONTBOZm1JMUlWWjhGVldVeWVTR2pkakZmcDVrTG1PNlRoTkl5UkJJZUU4dU5RZ2NmczZVR0tLdFF5MzFiS0RvSHZUdktUNWdrWHFzeDRyUTRtZEorNlRsM0tPbHppVHFpRDNtRmNJZUFSQko3Ni9tUktZUVdlTm5UdmVQMnYwN2hLb1VzOFE2MzBjc2Y2cFQ2cHVQZFU1R2lXMG00QkR0SVdHUTZ4amhrbFE0QzJGYlZrVVovV2I0aU85WXdHQTRVamppZktvUUFHVTBDalNOb0FsZGFreEFFMG9KQ2ZiUVN1RWRRK3RuK01jVThlSURacVlVNndTdEZrL085VjhPYXY1SWl2Y3FibW9oVTcrbFJhRTVWQnJDT0paUkhVcVRqYlZVWVJoQmhVbGt3aDZJUEdpbmlpRUE2YS9IRDM3aXg2QlhMK0RVaVpld2E5OEJYTDEyRFUxVFk4T0dEUmowKzJBaXpGKzdoZ3ZueitQODJiTmdCUWFES1d6YU5JZnR1M2RqM2N3TXBKSFExc3RZV1pqSHFWZGZ4MjBmK3hSMkhMc2JpK3FzVERQTFhVbkR5V2hDb1dUbG85VmZ4STBSbjcxQXc4YVRrTEg1ZWx2YURsSWhNOHpyRWRBbVpHVVU2dlBZaFZlWXpuQ3lQQ3ZNVW9NdlFBSzRVb2tTYzVsSWJGQ0J5NEE1WjY1R2FlaXhHVXF6S0hJMjNWWUNXcm03QlF5MEFKOXp0em1aNWpBazF5TlY5cGxKS0RmQU1TaElXdU1VaENWSzVYdFhWR2lWS1NudGJDbUFrTTRxT0g4NEJsUU5pcTM1aGxCcWN0QUVaS2d4aENSR1FwMDViZ0tUQ2pwU09DWlVZUFNjUTVWY3FCVWNTZ2hSaFFoUXMwTWo2bjNwaU1HTm9BNUJMS1ppTVVVVEFwd1FtRG1yQ0psTVdxOXBrMFU4b2t3clBSQkpLTUUrYXRNdUZza1BSNU4yakJWS24yTnFVWUlGQmFqWjN3MkY2RW9OeEtnUTh0YzJYUy9pRzMvK1Izano1ZTlqeCs2OXVINXRIbE9ES1ZRVlliZzh4UG0zeitMaXhYTVlEb2VZN3ZleGJtb0t2ZjRBTXpNejJMVjNIeVE1L1FMMXlncXVYTHFJdDk1NEEzZC83RC9Dc1E5L0NndnE0SkphemhTV3JUUXlHckdBS2FQYmFwdHJTdVFxbFlOc2JNVGlDV3RQVGtQMVJKekZiK3J3ZHk0YmRpa3QyTEkxMTlmQkhKSzhNV1l5NFdkU0tzMXFNc0RzYnB5TlQ5cTJZSWJLb2RJN2cxaEFRcjdZSTlNTllDMzM0K2tkUzJveXJzcHFQQkROZFJLWEtsNGk4OTRVRHhGakNtcFplMnVOWk1VdkNpdjFMTit3bzl1a01UMzVGSnRmdkR3MEdvc3k1VHFGU0VPcXFYQkU2Qk9qenc0OTlxbW5DNDVLMFdsV1ZORXdNRkpGTFQ3YTFrMDJnMVRWZE9LcjFkYVRvZkxNWi9ZM2wvUEpZYWNhVWQ1eTZiN0VVNzJqT2d3TGk1SDgwM041RTJvNWpYNS9NWURxR0hteG9CQ2lVcWxKakJzcGhnTU83N3V1V2NiM3Z2eW5lUFhSYjJMSG5uMm9HMFhsSEM1ZE9JZXpaODlnWldFSmcvNEFnMEVmVStzM29PcjE0S29LUklTTlc3YkFWUlZVQkN2TGk1aS9laDJYenAvRDFZVUZQUER3MzhXUkQzNE1DOVR6SnpvSW9DYXZnc1RjdER0THcyTGxZRGJJa3FjMFVUNmQxWnBhRmlpL0dQd21hK2o5WnVIMEhQSzZWWURGK1AyYmJJUXpxQnJyNk9MZjQ0d3Nzb2NWcFhRL2JrQTdCME5iUGZnNVpwUjJ3U1prWlFhZXRiVGhOM2dTQWQwWkRTbGpSSEd6Tk9SSUZuU21ZckNGWVp1MENSL1NyNWRxcmJ6bFdLS2NzcTFKVWYwYUhpS2RWK3BCTFJmUXVwak9rL29OSHp6S1FVVG91WEx6OTVqZ1FvTEI0UzVMZU0xZUl4aXhnb1d4UWdFdmIvekdiUlJnY1hsdUFTRnN1bXp5b0JiZklFbW5oTFRRVlcxcDFCTENxeTByYzdReUJBcnpFckpKZHhUNnBzNUFUZ3VkMndMVHNTcEZKUU5LbXV2b284RUxYLzhDdnYxSHY0c2RPM2ZpalFEbWFkT2dDcGxVYjkwMDJGVysvbWNPZ0poaU1EWEErdG4xT0hmbUhWeStmQVhhTkZDdElkVUFmK3VYZnhrYkQ5Mk9sYW9IMURKV2JvMHg5OFdPZVBNSGVET0cweVpqa2Q1dElNbzhmL3c3Si93a1dua1Y1aURJbEZxc216TmkzNVVUcXJYOUllbndhUnJMalFDaXhmVWRVM0E3VFNqUE85RFdlNWhzcU5VbVJCWnNEb2VqRlp5aHk0RzA2SkJBczZXMUl3bWNIS2M1OCtCNFJsL1hiQWpTVW5sN3poMG9MMXlwQURzYWsvSXc1NmpsaUFKdjdDTVZrNi81S3lKVWpsQXhZVUNLSHJQL093REhaRW8wU2hwOWNpN1FUbzIvcFM1c1JKRkVRU0hoTXh4R1hJa1pkRVNwWmszY01EWEdycXd0M3ZJMjNLb1ozQ21sRlNnOC95a0VGbzN2cU4xZ1VWb3phS3Zib1pRV01MSkdJS2R6N0RlRU50aXliZ3JNakl2bno0SmNoWDZ2UXRXdm9LSVkxVU9NR29WVEh6NjRjdDVCdUdtd2FkTWNMcDQ3and0bnpzQ0ZMR1Y1Vk9QaGYvUXJHT3cvZ2hWaFVHM0FVV085bllzbGMwSlRvSGdOVUl5T3ZSZTFRbXNHQnNlMWxsdUpwaTFkbzRjY0dmMkp0MHJuUEZtSzBKRXMrMi9raWE3V1ZCeGk5Z1RPR0lSUWE4emJtSVBTS2hpcG5USnJheW9FdmN2TmFLZG5kV2FoYWRvdmtqUUZ1V1FqckRFQWxPMlJtZlJWNDY1VCt2NUhFWXMzbkhBVVQveXc0Vm5od3FibThHK09HQlVSbkZOVXhPZ1RvMktDWTZTZjVSU1J4ZE5DVVVnWTNvbkR3dUN3QURsdzhZMzY5eE1WMC9wRC91ZkRZa2swVUt1RTBZZ0ZGQlFLQ3ZBSGFrZFRsV2UzZG16VEZIbENEaXlSMWRrVWpPeFJvU0g5RkRJcXdVZ2JRZENuQnJ4d0djODkrUmpXcmZPeVhXa2FhRDFDelF4TWI4RDdIL3JibUpyWmdPV0Y2N2grNlJJdW5uNE5aMCs5aHNIVU9zaW94dmRmZkJIckJ6M3dWQitpd0cwZitBZzJIenFLUmEzQTJ2amdtY0RkakJ0cDJ5eUNjdXVwV0g1TzdlQ1QwcjJaV3dLeFNUMFdoWXV2NFJLMWM5VHhHTjdWak14UkxTUkJPa1lLMjk3NGFtWTNwQklpVUpJUmpDWE5nMGEwY0R3dXFlZmNxZWN6Ukpya2pmRnV1bXZIekJtTHNnc1BpSXIvL0l5VTJhdzlBeWhzc1hQMGtzUnZvbWlzNXJDUm1BQVhhbFMvNFlIS01TcUV1cDdnQTBUSUFEaHMvSW9JRlh2M1ZnOXNhTVJqTTRMUFhsaVQ4MmpDVkFDRG1CZ2o4Vm1JNVB3QWtnV0NBVXV3cnNIU0haSkdHZ2hQTmpSVDE3UWozdTIySmtEUkdJN1pMTTZ3U0lUeTZSbFBUYzlJa0tFamcxUTN1dGFRRnBOditqckV5dWxYOGRsLytldHd5NHZvdXdwTG95V29LcGFIUSt6N2dmZmhvYi96QzNDYmQwSEJjT3dmZkgzdUpIN3JmL3B2UVFCZWZ2VTEvT1RQL3lJKys5di9BcjJldzVMTnBudUpBQUFnQUVsRVFWUXc3djJoSDhGOFkyWExDczVrZlBMbUsxSlZ5aHRMZ3pLT0lENXppb3RjN0ltcHhad01MUVRZdXVvaUwyZWFtdlp3QUxiSE14OU1iSmlCdEZDOUlJZmF5SmEyV3NHMG1EWFk3bG5JU2taT2F5a2VKSVh4ZVV0dlUyWW43NzdSYktLS1hRejdGdCtQYmNEMUdVTzFTcFhST3JQaTdEZHpha1hLSjhaTEE2cjRDNWV3dVFrOVlqZ0dxbERUVndSVXhCNE1CSUhZVTMxRW5wS3JBTi9TR3VXS3Bxa2hBOEt4aFBCNzJqRWhOdkE2S0J3QndqNDlyb1ZSaTJKRTRwMkRKSUlsRnJtWjVQSk9wZ0dJeG1nck5US1RwdWM4OURob3pod2k0NS9FSXNSaE4zanhoMk1PVkk0Rkg4VFlTYm5FQ2dqNUZIZGFoN2owMG5meHgvLzhmOFBzb0k4R1FETWFvWElPbHhlV2NQL2YrbG44d0E5OUNxUEJCcXlJejVLNFlSQTFjTlUwR2dDTEMvUDQwYi8zaTloNzU3M2dQL3kvVVkrdTQ2NEhQNGJlNXExWUVnRFNoTHlxQU5BTFV3dEtPZ0F1a1gzVWFDNmR3MkI2R2pxMURpTzRsQm13U1NvdExraEdPZGt4V0RadXBrb1Q1SzlxZldlMUpmdk5nWllTRm1PelhNdS9hVEZ0T0ovS1lqSVpZNDVySmQ2MG12ZEF5V2lzYVd6Rkd0MFJxRGlYUWdaaHJPK0pHSTRrWmNzVldtQlMwWXBxeFVWa3BLL0d2REtqNnBwVWUxSEFRMkdUKzAzUGFmUDNtVkVGVlJhVEpzVmE1SGVkcWVtS0cxVElaNzAwT05KTEhEVkdvWDNXRWFHbkFnR2gwY2dpUkJPNDBHQkVMZXJGaW5lc3Y0RXlWamRlMHlTSlZiWDBVTnYzbG9xSkRhNWV4T2pxZVZCZCt3WWNDQ1RJZGtrQ2k2RmV6Nzh5YXJEbDRCRU1CK3NSOGlKTTZRam52L2M0L3VpZi9WUE1UZyt3dkx3STU1elhCVXpONEtkLzZiL0dwc04zWVJHVlJ6bzAxc2IrQXd4bTUzRDcreDdBaTQ5OEhadjMzb1FyMVRRZS9zWC9Fbi82Ni84ekZxNWY4OU9BZzhvbVM1dXQvMk43M25rQVBJMGRyNFBnOUV2UDRQVFRqMkRQMFR0eDRQamRtTjY2RXl2a2JjUTEwcVRxMVorNXY1NFN0eDNmV0l1TlpqTUZTVU0rYlVWTzhabUhRTUJKZ05RRkliTThvK1VSMVJvV1kyY0xjSGd0VGU1TnhuTlM4MEdZY0VZYVgyQ004NVJ0ejRpMHR2ZWQ3RUR0dENNdGZBdWl5cFRVS3hKZHlGSzh0SjdnUHZrUC81dGZiVTltVFQxNEFWVVhFNk00aUNnb2JGd09nME1kRXh3SUZURWNGSlZqOU1Ib08wYXZjdWc3d2hRekJoVjdWSjl6NnU4NE5QckV1eFY0LzBqVDI4YU9OTG84UlhFN3V5bjM4VlBBRGpneERua29xSUxRaERwQU5VdnExSWd1VWp1S2FqRk5VUExkTVhWZ1BrRVNwVWRjZEFoR04rWDB5a3hvcmw3RW4vNzZyMkYyL2h5R1owNWovdlRyV0g3bkRZek9uVVp6OFIzUXRRdHc4NWN4V0xtR1RUckVVNDgvaW4yM0hzRUtmRTArZk9kMS9NNnYvV05NT2VEcS9EeHFVVFN1anpzLzhrbDg4aC84NTVqZWZRaERWT1dnalBCNVFRd2hZSGJnOE5JVGoyRGY0ZHN4czJzZk5telppbVo1RVM4OS9oMGN1dTFPOUdjM1E4QnBLaEZNTVZhc0djUHZGNUp6RVd6YnVCN3U0bHY0ek1PZnd1VlhuOGRUMy93S1NCck16bTRFT1pkY2hTblBJMi9aeXZ2N2xRNkVvQWRKdUhQUm1tMDdnWDMyQ1lKcDBzRVl5clhyOCt1RlNWVE93WnlRY25zUVBvQ1QycUxKdFRTRXpSdWZKN2JzMGhpYnNTNHVvTVdCUWdINGhpbllra2FFUExCZU1ZRmRadG42bGRFQnBGU0hETEljRUZrWDZaYklPWWZUMis4NW4ybzdvcFR1eDcwVzM5UUZPcThpaGlQL1owb2JoN0tDTUEzdDhOVTRJMHNXRTBWa3pPQ2piRkxCNldUellxYVFrbGNFRWdXSDNGeEFxSlY4ZVJDbHVNeEJVY2xqWEtIalpPUUlBSEZnSExSSWlUVkpjeW1CSzVKUVdUWXRRaGxMWUNMMDU3YmhvYi85OS9IczUzOGZ1M2J0d1NpQlE2RTBJdWVOT3duWXMzTTdQblQ3SVR6MTdTL2g0QU1QUWJnSDdVM2hSLzcrZndZQ3NIWFBYbkIvZ0EyYnQySnEweFlzU2JpcktuQWN3UTlPeUh3Y0Y3empwbHZRbTVyRytUZGZ4LzU3M285bHJmQytoejZORTA4OWhyLys4ei9GSi82VGY0U3JHdFZuUWFyYkdxMmVZT0JpWVFZZ2t4bTlqVnR4VVh1NGZQVUtIbmp3QVh6b3d3L2c1R3NuOGRWdmZBN1grclBZZmVRT1RHL2VnYm8zd0FqT2x3NWh3SWpYbDVXVEUwaXBITFRSdGgyM3lrUjcyaWRnVHNMekszTmZBdWZ4Uk5Da1V4a0RyUldEMXhtNW1ZNEtnVUNwN3FNSjdVWmFPRk5wU2tjNjVyTnE3ZVF6a0JjMUxtQXZ6NG9CbmxYaG5NK0NLaUpUZ3ZzTXVXTEFmZW9mL3Nxdnh2SGZUUG0wMUlBWStoNEVTZFVyUitDT0EzTFA4S2Q4L04weCtxem9PMHIvMWcvWmdXTUNNK0NRVDBvMmYwYUgvWXd0bEF4dEpVNCtHSVNIS0VnWlFkeHNIRlBlUlAxNFhVQ0QwRE5nREVkVEZoSzJwMjhsb3VSeFhjcFg0MHdFVGEyclNEVmhic3hndVBBZzRvaHFTcFJSekZhVUhMYnMySW1WaFVXY1Bma0tCbE1EUURrMGJ1VFhFeVZjbTcrR3FjRUFjeFZ3Wlg0QmM3djJnbVptc2UzZ1lXdzlkQ3VtdHU1Q2Y4dDIwTlE2Q1BrNjI5OXZEaHkyZjAzL2R3cXNpMkxUb0k5VDMvc3U1aStkeHozMzNZZGVyNGVxNm1HNlVqei9uYTloeDg3ZG1Gay9nNTREWENqakhEUUFpVDdGNzVIQWFZTUtEU3B0NExSR1R3VTlDQ29kb2NlQ0xldW44ZndUajJMenpsMjRjSFVlNnpkdXhQdnZldytPSDlxRCtWTXY0M3VQZkF2TnlqTG1OczZpNnZkOTF1RTRnR3BOT0h6WXNDcEdHYWZqcEdsVUt2b1NlQmRwUEtOVTBlek5rQkQra0huRTVodi8vTElVbmdqcHNPczRVTUZvNzYxcWs4YTBJRWZUR1d0M3J4WmlOS1dqbXJKTHgvaUJHMUNSU2IyQ05tejZ2bk1ZVklRQit3eTg3eGg5UjZCLzlzd1pSZExKWjVWVWJQYUpLTFEzbDVUUVJocHFleUs0dURDUU00RUl6Q0dwOStMZktZQitnUnNtSkFWYndvV3BOQjNKWmd0dGx3K0ZSbFdkVURMOEZER2RZSW9rR0dvVUdJcGdwUkUwUWhoS2d6cFFhRkxNWWFQVVNBVFdWTStsTnBhSUh3Z1ZITDJHRTRzb2lFcUZvQ3daOGJWMFdlVDAxVCtnMldZSlgvdTkzd0xOWDhMVXpBYi92ckdQT2xLV0llaTg5KzY3Y1BiY0Jjd2V2dzh6Ty9haGFRTGFyT0U2UW51MGFvTkdKQmlETktqcitIY0oybnpCQURWZWVmSTdlT0diWDRhRG9KcWR3NEdqdDJQUGdadnc5YzkvRHMyVkMyaklvVm8zaSttNXpUaDgxM3RSOVFjSm41RzZnVFFqYnhSUzE5NnNaTFRpVTlGR3duSHMyN2w1TklTT2x2R3BoMzhDVGNCK2VoV2o3eXBzMzd3SnMrdG04TlpiYitGcjMvcHJuRzBHMkg3a1RrenQzQWR4Z3dDRVppWWxaL1BHK0pJemxxUFc0anUyZUlaR0d1OFRRZVZBRG9QM2x5QmRxZlNLaVllS0I2MWp0a2FxaFlaZk5mNzdPTnVNVmg5SElRaTNnMU5DVzd6cEtVZ25XaEExaWxHQkVXZUttMWw5OWgxS29FSGwwQ09nNXdJV0I0QWQrMHo1TjU0OTYyVTFKcTBtSWpSSlh5Rm0rRU5JRzVoUXNVTkY0U1FnUm84SWxlUEVYNmN1cG9DZk9XTERGa1JhcjJ0M21ERXk3NGJyVDFlVEdzUE10UVA3UlI4ZGdpbFRUQWx0ai8zMEFvd2dHTldoRkpBR1RUU05VWithUjQyMUtMSndKNkhCSGd3VVpHZWFKaUhZak1acUk4S0dOTVJUb2hnbGdLUzVoWlhRSjhIczhCciszZi95cTVpZDZrR0pVRGMxdEduUVJMNDlpQllxNTdCeDR5d3VMeTVqWnRPVzlDSHlpV1JVbGlFUWs1M1pJSkpNTFlpQktwd1F6QXpuSEthbXAwQ2ttSjZhOXNFOHROUXlnT1dWRmR4MjdCaW1aNmJBeElGMWljMXF2c3RTZzZsSW8vN1BDdklCU0FSRFVkUkt2aG94K25jUmhkUTExazhQc0s3ZngxOTg0WXQ0WTRud3NaLy9MNkRyTm9aK0RvWTFoTlVXUTVPVm5XYSsvRmhvMi9Zc1NBbllHYXBPazJkRGJxQ0pucE1LU2MxajdYZXdMYmpXQWs4RE9KMlZpMnJLS0RhRHc2d29TSk5XeGVxT3FUV2owNys2Ui9aZDJBdTlpb093RHBnT3AzNlAvV0hkYytFZzlpVTZsY28wMHlWSFJuR2xrWmJqMUJvSGprSWU0MjJXZTFyQzY0aFBZYlZqbTBXWmRTY1V3enhoVkdIVW12ZmFKTkVzWjVPSjhMQkVTK2VYS0tBUlZUUUFhdlh0dC83azliK3JNZ2lWMzhEaGU5T0pycVhCaC9mUTg1OHhYVlBzd29wcXF4ZzQySUJBeHVJOEwweE5ob3hETUlhREdkeDUzdy9pK2hzbnNHM1hMb2dxNnRxNytrUUZsd0J3bFVQUFZUZ1E3bjNUQ0pxbUxrMVlOSno4UWJFb1llUDc2VDZBU0JPeUFJSUkrVzdLV2xFM0k0eHE3L3h6b2I0Y3ZBSTF0TUg2d1BmNnlaT21VNGtTaGVrVEZzOWlOQ0tvZ3hPUnZ3WWZEQjBwZHUzZUMzSmgwakRuTVhKTjNlRHkxU3ZBdWxuYytlQVA0NTZiajJFZXZXSm9hTm40b2kxVXZSelU0YWxHMDZLdEdhT0pKWnNHck1WcU5oQkt4M3d3UzU0QWxGZ1BocG13R3BTWXNjY2xkVFNGNXJMY1JsNHFvQ2xsd2JDdENLb0ZGY21taFZjMDNETnRZd3lTUzJzb3FvclJaMStXOXh3dzVWeFMyZlpDQ2Mvc2Y2OHF5dkFVTkxTN0p2a21GKzFwN2Q1K0RjdGRGQmlGRk5SU0xYYldRUlFmeEFZV1o1bnhzdSttR0RDcFpBWU9COTF6ZTNCNEV0VnF5NEVuZXQ0RlRFTkFhVEZIdXREcnVxWGw1U2NBT2E5aWk5NUNFWUFKeXdRRW9Ja2lIVEh5MEZ4R09lTTNFTHZOWWlZVWE5SGtmU1FOQnBYREsyKy9nMHVYcnFCdWFveEdJelNqR3FKTmRrTU9IMVFVRVBLWlI5TTBvU1JpTURrUU83QnpYaVpkVmFHbnZ3SlYvdXZzR0k0ck1CejZ2UjZ1bjMwYjh4Zk9ZR1ptQmswajJIWHJNV3k4NllESFhvZzhmZVFxM3pqa1hQaXZncXY4bjRrWXpCN3pJR1p2MThiTzMrOXduOWZKSWo3N3ovOTM3SkFtdEgyekx4MUdRNXc3ZHdIVE8vYmo3ay8vTExZZXZCWEwxTU1TdVNpUndIaGx2R0VGZ250TzdpNE82anFvTVE0cHJiZGdxT1VNTW5PbTFBS2lyK01HOEtna2ZDb0YrUGpKeE5OdHBOa25Jb3UyMUtnRU5UbkNLUG1PUURLcVR6Smdxc2JXOHlqOTUraExJQUdzREwweUFDcEg2RE41MWkxaWNzU29XRkc1TEw2TGgzQlZFYU1KOVpFWWg1N01YMUlDSm1MZ2FVSnJMb1VlYkVuOGYzYytZYnlaRWx4N290c0pHVmtsQ3NNR00waFRZc0JRQTlGa21qa1dFdEhSTmttU05hUHdqV1JLS1BXQ0o3Vk9rNllCSldxRnJIOTc0ZDFjR0Yrb2pWQnRoN2tnY2tuU1Z3cHdwUnFuNEJEMW1BZ2tJeXhkT0kydi90bG5zV3ZyWnBDT0FralRnd1RUamdncU9TYThjL1ljYnYvd0ozSFhoeitPSVVYN2xQZzlER0xQKzBmWFdaK054TTJjVzNPRmdCNFJYdnJxNS9IR0U5L0MvZmUrQjF1M2JjVTdiaE0ySDdrYlEvRTBYbzJzbllpaTNjaURSKzFINGVZVG5tRWROb2ZUR21lLzl5aldUVTloV0Rkd29oaXVMT0hjeGN2WWQ4ZDc4WkVmK3psczJMRVhDM0M0cGw1TXBHSUhWeUswemlwYU1GRnVBVTRLYTM4aVcyWTdLUUhKU3E2NThHSWdhNmlSc2p0cWxST21IMk9zU0RHMzhWcVJFWXpVbDJKWHFFcVNBU1hORGRTMCtPWkdjU0dqVW8zV2RXSHpxL29TbkJRZ0IvVFlsK09ES0xpRG9zZnFkVGVjOTJnTWl0VlU1VkFMWVFSRkEwR2o0UmFKR2t1c1VGTkhXZ3lDT2dCckxPdzU5K1JzVTNZNlpjRUN0eFJRTmhWQ1loMjA0NHVROFFJTnd5cHlseU5CVWlFaFhSdFBSVkF4bUVnc1l2cWl5NTdGT1BnaU5mSGswWkhlMXkzY2VJaytoakdqTVpZMGtYZTFlV1FlVys0ZlhrTUtDblVmUTdCSlYvQUh2L09iMkxOek85Yk5yQThwWmNBd1JFMTdjNE5yVjY1ZzIrRTdjYzhuSHNheTY2TU9nVVZFSVdHaGtSRWpsV1ExUUJKVFU3K2tScXdZQ3NET1ljUEdqZGl6YXdmZVBEdkNpQWpDNW9uR1VpQ1dSeVJKSUVYazFacmtDek1vMUl0OHdwdE95UWhQZk9VTDJMWnhJNVlYRjNIbS9BWGNmUCtEK01SUGZ3UXoyL2RnUlJuWE5BUERFT25ZZnl1UHNmSFdMUGhKbmJwcXhEQ3F4UWxPQ2RrSnVhdmFtcDlhRW1UemU4SjFVTGdVdFcrdXRqc0xpb2t3bkkyMjFYczMyT0xiK2g0UUJROUI1dFR4eDFvR0YwcUs4c2lzQllvdlpBQmViK05RT2M4R1ZKeFAvY2hpZ1JqVlZBVU1HMS9UYU9NM2pMZDFJbjlDcHJiS0RHSTJ4dHloSVFFMStUNnhHRmZib2dHaUthV2pxWCtnSGFrTjloQmRhNzI0dVFnSVFwbXVURHB3bzdsWDJ5TnE5ZWxtVUdPMzM4djhybEtHazZCd0U4MjJUUUtMUEJ0NWNpdzdUSVF2NEt2UVU2NEFwclhHbzMveEozQ0wxOUNmM1lpUndITzNISXdpSXFMWENCYm1GN0hvcHZIRFAvVVpYRU1QRlFoVFd1UGlxVmN4V0RlRDZia2RHS0xLSjcvaGw2TUl4UnVwK3RkMFlOL2trNERTQnFPZ1NrUXlhTTVOVlpycTN5Qkg5bEF4SEFqVGpqQm9sbkhoOUNtc2pHcHMySDhZRFNwQUJNc1h6dURpeVZleHNHNGRidnZneDNELzMvc2czTngyak1EZVZ6QTJ4Y1Q2anFuSUN1MHdtR0trWm16SFRRWStiVitGbHJsRzRMYklyR2NmYjlSTWNOWlUrenR3a3NGblF4dGJmRXBSbW5EU0hXcjdRNFJ0MStRcDE0cEMxVm9Na2RYOEdheHhNR251YUZXVndoL0RNZEFQWUh6ZitkOTdnWVZ6YkROdTQ4UUVSVFZGN1BVREVCQ2M3d1BuYkx3WmpTdklkTEFwT0hNUU5sVW16N0ZiVTBXbENUTUhWRXVwWllFdmlISEJNUnZIKzBQNTJqcUVmWlhJTkdqeWY4dXlaU3FQUDQzR0VyRSt5MllkeWVUQnpuaHFhVGJpYVJ5VmZpTFd0TVBJUXpnTFpxaTRodFk5R0M3aDlBdFA0ZVh2ZkJtN2QyeEhvelZrVkFNMUFWeWxtclpSNzlWLzV1bzhmdUtYL3p2VU01dUNiWm1nbnIrQzE3LzJaemgrK3pGODkydHZZOWV4dTdEOXlISFUxVlFCcXJLaU1NNms0RFdzSElJbGthY082eEVhNFlEVXgvdXFxWTFiVE10dDlOVHJqeFp3OHZGSDhkcFRqK0t1dzRmdytsdG5jUGRQM1FUdFZTQlNYTDQrangvODhiK0x3L2ZjQzUzWmhHV3Rjcm5aTkdPTVduTTluaUpyY3NubC9KbFNRSWdIZ1hWeHN2clZBTTVTay9QVDJNb2NkQ0RTTVdKaEg5NFN5czdtUFhKYnVta1VMMzBuSWgwWkU5K2dkMHdIUWxTdWFwT05SWTFsWG1XY3lkVFlmSEF4UnRETG1XeVBUVCtVQUk0SmprTHFuNEJJNm1nbHFwN3ovbHlxd1grTkdkcElHc01WSmNIWmZqbWJFQ3FhakhnU1pSZW1DTFpGeFpKMmRjOGRrRkRVU0drcG5hcDVWQ05sdDk1QUhTV1BPY2txcnR3MGwydnR5Qlk0c0FmUFdoTjRGVUVsRkpwem9tTlJ4UFE1UGdqaW9LNVRPQTExdFNxYVlyS3IvenhjT043WWpwZHdjb2hpK2NwbC9QbnYveTRHcUhEeTRuVzQzZ0RrSEZ5dmgxNi9oNTRicENtL1V6UHI4Uk0vKzFHNHJYc3dCQ2NncVJtTnNHRjJQUTdzMzRlNzc3d0RiNzl6QnM5ODcwbHNPLzQrQ1BjTVIxMmVWU3FTbml1bklhYVVXSVFtZFIxeVN1d3B0RlUzWVhPSmVpSFF5NDk5R3hlZWVRVGJOczVpODl4R0xLNk1rdnBNMkdINzRlUFllZmdPakVDUVJIa0ZaaUhjNWZqbjFPbXB2bGNqdXpFRnV0ZXMzL3hjMUxSem0rdzg2Zjd0Q2V2Qy9JZ3cvekFhYUJpOUJxQ1pmMWNkMDlNUk03MXNLRU54anhpMklHY2FtZ0ltY2RTUWhxMG9uRjJsRkVIMFpveFVqQUdxR3RiQXo4VlFPSkEvOWNrTGUzb3VaQURzL1RRaTZNbTJGZEJrMjVWem9TSlNoMFlGdmZDd2g4bkhQZ2dTaW9rMm1uWGFVZEdpc1FVN3AwcGltaFNrcGJwR29OSnk4MEtvNU1XNDkxck1seGhOMERxTE5vbFhOVGFlT1hDSUpndHlQKy9PdjBhZGJySnJSYUYyZTY5WDdjVWFzZWFRR2NUSUFQakJHMEVneGJEYWczeWxPcWJEMEtxNzFtM2VocC83eC8vRWM3Y1J1WTkwS25Qd1A0emNObU5GZ0NhS3RvSkhvNHFuNEs3TXorUEsxU3M0dUg4L25uNzdCQ0NTN2N0Tjg0NjBQQTFTOTZaem5xRXdEUzErT0dvd2RqSG9hL1ppOE96RmFHRUIvVUVQeTZNUmxwYUhhQUxOR04yRWZUczJGUWRLWnJjME5WRGx6a3pOV2FMWkFQNmFKV1VKa3JKTEIxSHgzWjlSREVYV1B5Q3E5OWhRUFFZY0pxc3R5SE1HS0dCSUhMTVI0My9CNlRPM09yMmpkYmk1VjdGck5tSmRiTG9TaVEyM3IyTGs1WGw5U2dMRU5lTUFxb0hLOW9HMjc3eWpWbzhBUjVMV1RWdDdGSU5Mek5BclN2U1UxK2hyQkhsWVVhdmtrNTlLTjhBSU5xVnd5NEZENTNBNk02ZXoyNHZxSlBUZVp5TU9vZ1R2bFRiOXNiWVBUNUZENlJGcE5TRUhScmFtS2pZYTJXYnhnTUpTTkFHVmdwYXg4LzQwY0U1a3JjNmlqNUJtN1h1YSt4TDVXSkljenpXYVc1TFJiR3ZITVNjOUZIWm9wbVlEa1JCTkxlUEpZUFpDMURnWVBsenRHUEhJUExnZUt1ZHc3ZXhwN0p5L2hON0c3V2pBYVF5WUZzTkxjakxvaUwzcmNxRHl1SUREd25jNWZ6ckdKaTBIQlkyR1dMeDhCaGZlT0lFTnpPREtvVzc4Y0ZJSjk1NnNPaS95MmgxRnBCWEFTT2pSajlRc0o4ZWY1SURFVVJZcnVhWkdCQ21EbndLU2ViUXhDT1hrOFIveG9lVFJhQzJ6amFkZUZHMzVrazRMcTNFWXVpN05nb2hmQzU0WXRrRGtaQ3BMUm5wZkdVK0FLdHI2K3V5TzhoQVppYUtSa0xFaEJKVmU0UHNyaXIwMlFReEdDbFpxVGZIU3pxekJLdHAxT2ZXZ2dVQWdRY2RMQW5DMDFvbzNKTHdvaHcyZXRtSW9HVVFWd29wR0ZZMkc5RXBEWmNQR3draXAwK1pNb1QyVUtIZVdaY2NmbzdLREVYVVVNOXd0SFdZV01XblNIY2JJbnBJK3BhSS9YRkdZeFdRVVAxbE5jZXJ6OTdkSlREQkJwc21JMDZHUmV1V041b0VUOTR4V0ZrS0ZvVTFXa2VYVEo3MXVxTW1ieHN0OHdjQzE2OWZ3a3ovOElFNmRmaFhQUFBaMXJOOTdDTHR1T1FKTXoySlpBV1dYcEtSRURJY20yV1hIa3FwaXJ5SnJOUHZZRTRBZUNhYXBRVDEvRlcrOThqSk92Zmdzc0RTUHpWTTlOUFVvTUt0ZURDU2lScXVhQlRuV1Z6dVpiZHIyWW1UVXZ3Um9LVVBmUVBsbnhKa1ViRG91QmFYbGZabDk1YkpJVGFsV3puQ3dqVHBKZ0JQeEpyWU1sZWIyc0hBZlhkVFJCRDZUb3dFT015cU5tVUhzTnFRQStQc1NKYWxYbWZ6RUpqRHF4dHZZcFhPUnZMZURwL2RDemM4VWdua1VKR1U0aTlWVTZBYmJxdUx0NE5EYzB3K3BDTmZleWt0Y1ZZeFVjaUdsckNMU0hCRFVKZ3ptRUZVMG9xZ0RpdHZFRzZneDFaTndnWndHWDVReXpiaFpzOU5yS2hFS1QzTzIyRjVoRDVXMTI1Uk1FRElzeUthTEVDMWpobktpYU5SNWtLa2w0L2VtNFJmR21UYVhOcG9HYllZWlJVbFNwYkFUYWFnYy9VeHRXYXY5SHVNaUU0TVkrM3RUMTdXZjZVZkF4Y3RYY083TVdlemFzeHVmL3ZEOVdGcGV3cXN2UFlMdnYzTUpXMisrRFJ2M0h3VDZNd0N4bDRWU2FBbVZLTjFWT01lb2lDRm9RQ3B3MHFDNWZobG5UcjZDVXk4OEExNlp4K3pNTkRaVmpPc1FYTDE4SFJzMmJnQVVxTVZQSTA0TlhFWWdGZWMwd0Z6dlJDdGF4YmdobEJOK2NZRzJ0NjNBaW5jWk8zVXp1MktyZG1jVjVpYXZUQjlyd1VlUWxTVUZud3RPZ1NPMnkvdm1IRW85TTl5V2I2ZlFGVnJXRldpVS9YNEt3VUI4R2dGRmNNdG1SYy9scmxzS2g1THZCalNlU0tSRmtod1B1U3ArZkZZMmhvdU1xdExRejZIQlRzdURJbDVNNEU4SUJKdHJKVUNDVUdNa0FjVDJEdENvaFh3cHdaa0lrOUJqb0dwU3NRSkhNdzlCYy9ZQm85cXlyc1EwWmk1WGNwVXhRYU0wcEVReHdvbTFIRHhKOXJUdEdIbVc4bVJORWw4dWVyVFpESXpRcEhtUVJGY1daLzRZTnh2N2I2UWwveHoxL0ZYVng4V0xsekZhWG9HckdBdE5nKys5OWlhKzg5ejNzYjRDYmpsNEFBZjI3c1dQM3ZjRHVITDFLbDc0MXVkeG1hZXg3OGh4Yk55OUYxT1Z3K0tsOHpoejVoMDg5c2lqMkxaMU02WnZPbzcxV01Hd1hzRTdyNzJNRTA4L2dZV3piMkR6eGczWTBPdGpSUnFjTzNNR1Z4ZFhzT1BBUVF5WVFVME5WeEhPbnoyRE44OWR3VTBjdzU2TWJZRzFqamxqaDRpdXlSMFRwU2crUGpjcTNkcjBYYjNNSkU4c0ErQzJsR0ZzV3RTVGtFY0ZqbjIvUkM4bzhYb2hHRlNoSFRjcThwZ3BpNEdDbWFsS3lBQlVJQXpVamFCbVQ4SEhFV1VjalhOOTc2bWZuUkZLak02a3NMYW1MV2FrM3poOVJUMFNHVnRtdzdEdGNFMWlxQSt2NXc1T0wyemFNOVJ2NWdZQ0VVS3REV29GaHFwWXFRV05BclZrSkZ6TVp0UkFIbkFhejVSVGV3V01EcUIwS01yWkdCbWpqa21QclcyNEpMRE52V1FDZ0l5ZlQxMjh0NFNVaXJSVU1DTFFreVI1R0VOU0RyWW0zRkxuQkpLeDFrNUtwY0ZvY25aVkFUR2ozd3p4MW5OUDRJVkh2bzVOQThLdVhidlEyM0VRdDl6N2ZseTdjQjV2dlBRQ1huenMyMWl2SXh3NHNCdTMzMzRITm0vZWpJWEZSYnp3eXVzNGYzVUJwMS85UGc0ZHZBbEhEeDdBN2NlTzRIT2YvMHNzdUhVNC8vYWIyRFE5NVFHK3BTVmN1bklGVjY4dllNUHVBN2p6L1IvRTNsdVBZZDNzQnJ6ODZMZnc1RjkrRHNJVmR0MXlGRGZmZlI5bURoeUJjczh3cXBKTE9TMVIrd2tlV08vU0ZKT01XYTI1MTNhczJMditSYVhZSjFtQloyclV0NUdKR1E3aXMwd1hPbVo3UVgvZlkwS2ZDSTZkUDYzSm05dkd6bStPemhnYWg2S3lIKzhlMnRjYmtaQVJTTkRoVU9qeDk0ZHhqeGt1QUlGTUVYL2haS3FEdG5OQ1BGQy9lZnFxUnBGRWZBaE5HUFFwaHRwUzQ3R2ViTHhpSlJ0R0RqVUlOWXNJR2lpR0RiQWlpcnBSak1SM3RzV2g0NW4yeVhWTkZIb2cxS1NsMFJFVmpyTGpSTWV3SnRXYWFVSko1MzVvY2paRFRDazE1VkJ1UkNKazR3eWJVU0FEZGF5bDBpdkxrdlBpaUZQSVN2V0lUamhoN0pHb3FVd1k1MVNMQklCeVF1SjdxTEY4NVNLdVhyeUl1VjM3MGQrME9UUmpDcXBtaUt0bjNzTEpGNTdGRzg5L0Z4djd3TUVEKzNEazhHRmNuWi9IdHg1NUFsdTJic0doZlh0dy8zdnV3bC8rNVJkeCtmb1M2cWJHcFl1WGNPSFNKZlEyYmNYUmV4L0Fuc04zWUxCMU8wYUIwcXFZZ0lXcldMNThDWnUyNzRUMHByQllLNXBrQ0txVDl2ZGtVMHVib3hhcXU5WDMvamdIalhZQW9KWnVyM3RjdEUvODBnY2k5clRFOHNUTDBDVmNLNFBRZ01uNTdqdEg2RE84TnA4WUhPaTVLdXJ4Z3k3WHRXZDV4Nm5iZ1dvVkpkL0lGWWZpaGtNNUhzR1Zpd0NncGdPYURadEM0QUFvYXljVnFMVHdWZU9NTXNiVUpqWUZLSWNaYXFGTmtrUFhXVURDWXlydUFvQW42cDJEcXFEczBzQTlTNkhscDBTYnBDMFdzdzVxOTJnRkJXSzYwY1pwUmJ1Sk9RcXBaVGc1S1RzWWs1bnpvNWI2aWFCVjZtalVjbzVnK3JPMVJIZUI2TXIvbnV5N3JkMXRxNHdnTXljeCtycG5HaWlEZndvdERDdGlmenNsUE1KaEdRUnMyb25aalRzaHhGZ2Uxb2sxWVhJWTdMd0pkK3phajNzKy9CRG16NzZKdDEvNEx2N3Q3LzBCK3IwZXBqZk1Ra1FnVFFndkluamo1RWtzajRhNDgvMGZ3Z2VPMzRPcEhYdXhJQTRqSlN5ckMrSk84UmpYMUFZTWRtM0VBZ0hhY0JydFRYR1V2TEhoVHEzY0hhRlZWdmExYTZLc1lxTkV0UnBCSllwUmlaUTlHNmhWU21pQkRkQlk1ejJLb3lQVGFMZFNzTWF3SGE5TllTTlBnWDVrRHFQdGdocXZGM3d6S21QU1VhaGRDNzJJNUs3WkpGSlNVTkFsYW1waHArQzE0Rkd0TkZBMXNmS1NEMVFvcEJIVDlKU3Z1WW96MlhOcWs1ZC9WaDFKYWZ0TWhqOE9pdlpJVjRHakM3RHhFNGdOR21GYVR5R25MYnc0S1ErWE5FQ0xDMzl6RVZtM0dtSnRlZkJiWHdPWXliS0J1MG16NTFES1RHT1BnNXJ4VUtTY0ZoekgwV0hhYXBoU05VMGZtdFl1aDFMQk5nREY5SlExTWhOdEo1a1NDQ2p3NnpUUk5xcmVrZ3Vxai9ya0FHbmdpRkEzL2dTV0tKdFd3RkdEcWdKR3k0dTRmUDRjM243ck5BYlRVM2pyOU51NGRkT21NQW1vUVYyUHNHUEhkcHk3Y0FFSGJ6bU15eXVLcFlYckdOUkRWTlUwUmdKajRLS29GUml3QmdkbnYvcEVjaVFUTHFmN1VndUpzK0trOUgxcWhxT2tUTTV3MkdFT29rc1lnOC9LZHR3QUFDQUFTVVJCVklIbElpdGRLTzBsRHgrMjFHU3E0ZTBRbHF5MTV6eTNPUmpRZGdkMytxZm1BajNwMTZodnlBRUdqakJ3d1FiUHhjM1BZY095Q2VaMmVHN09kd3NTbi9OSU1JRnJ6YWIwYXpCYThtbXlqNCtpUEFsTllEbUlKaW13TjlBT3U1SW9pRDdVaUgzTUNFclMwaGpkaUR1c1VYdmtTUjJNMFNiN0tCVmw1dEZCcHhFSkdRUFNaa3Qxb29Ic0pJM1c0aVMyNlF4cFUzU0djK1JvSjYyV1pydlE4a2ExRm5MUkRwVmo4NDhaOWVRQ054OEhPeUR4L1E3RmNFeTJUU3ZhS2xlVFEyQXVNSXhUY1o1VEVPNkx4azdHVUtZd3dOcGdtZ25ONGpXODllckxPUEhzMDdqMVBlL0gzTUhiUE1wTGdyNE1jZUhrcTNqaXFVZHgvdVRMbU51d0hvTitIMU85QVpnZGhpdERuSG43YlN6T1g4VzY2UjUyN3Q2TEwvelYxM0RvOEZIY2QveG1ESWNyT1BuWWwzQm1XYkg5bHRzd3UzTWZScTZQR2dSR2piZWVleHBYMzNrREIyNDlpbTE3RDBBRzY3R2k3TldZSnRpMytEVlFBYXBHVzNVZklJdUtWYzBrSkRMNWtKYVRpYlA4bkRxY3ZwMEhrQjlEMG5pYkE3QjBoK1lVbEt5VGtHUVFMV1RJSEZEM0t0VDZYcGpqZ2JrcU5PdEUwVmljNnN2SklDUXpVOVQyQkNaa1UxczFEc2twZkVnZ0hNMWdYK01jUmtYdmkyR1RRb0NnTDUyNm9zbU5vQmlzSHVzSm00MGJZMDZiUW9YbUY1Z1RKK1lOb2dqMUM3S2JqdGlSMWo0UnJzTzAzOWdCVjV1MFBDckZNbGJoTmVIZXpDTzd2TVFObFUwOXlobWZZcnJGc25kYnpqZWlrSW1FREEwbnhqTk96RVlPNGlKakFxeXFMY1ZVWmkvRVZGOGN4UzFCdGNiSlVNSW9FNjFXd2RnL3FlRnhtQjBHelRLZSsvSi93Q3VQZnh0ekc5ZWozKzloNjVHN2Nkc1BmZ3lYenB6R3FlZWZ4aXZmZlJRYmVvelo5ZXN4cW10Y3ZIUVpDNDNpam52Zmo5SEtFaDc3NmwraEltRFRoaG5Nckp2Ry9GRHd3VTg5akN2WHJ1SDB5OS9EemszcmNlem9FZXpjdmdVTEN3czRjZW9kckV6UFljZWhJNWpidGhVdlBmSU5ITjJ4QVgxMk9IbjZiY3l2MjRxYjcvOG9WclJuUUdTMW1YN0x5eitYYXcyOHZGV2ljaU9PSUVzTkc1cEJvM0xZenNSYVBrNkJKbTJWSDFvaXZBb3hiRUlNNEJyM2Z0NVFLUnNMRTRkWlVDRmE1SGs5ZnIvaTBKS2JVMzhPREZzS0htWThlbnEySFNnakgxUUN0TXJYTEg2eTN4OUxhZXA2alNjeFhnd0sxVWhETkRBZ21oMURuT2VTeDFlZzdseU1DSTFiRloveHNZdUFSTFRyMW1EQlhRVVpxNFF4WHVLYjNpQXVtekZLNlB3VGJWblJTeXRGUWw0b0hQTnZ6cWMvQmFxRXVEc0ZJZDF3NXFJdDJhc2RPZHVHS3lVQmpTclpBYmZKbUVISjFnZTU5blVGTDVOVFpJNWxUVFNrS0lDd3ZCalluR1lhK3RnZEs1cjVlVHo5MWIvRUxZY09wcXpoOUF0UDQrVDNua0Y5N1JMV3oweGowNkRDNWN1WDhlYVpjOWg3N0U3YzkvQ25zZWVXdzZqV3pZRHJGU3d1THVMeXFSTTRkdmhXN05tOUU2OHRLQTU5NElmUUtIRG5SeDdDdVpPdjR2R25Ic1g4TjcrRHZUdTI0TFpqUjdCMXkxWmN1L1lHM256NVNWeDY0eFN1VHgvRjlQUUFEejV3SDc3OStMUFEwUkRvOVZKeXhtM1UzNWpNcU9sWFlFVUJjS0hGeDJmZTN4ckEyblNyUGJ6VWR0aHBPUllzelEwczV3MFc3WG9TMVlLU05SZ3V5NGxqSDRWakRoWmJRTVdNSGpnTXArR1F0UVQ5QzBsMkVtNDdYS005c0RZeURka3lpQW9HeFFEVk1UZlJjbTZWcUdUOEtPeExPd3F2R2pXU1VwenNoQklFRFZyV3BCeGJHdE9lWXpNVlJoSnJvT2JvallDS25iYVNWRWtVVzVVa2lTSHlGSk13N1FjNVdrcjhqTEUzT2tiQThMT2NsSE5JR3ozMTU5dE9YNEt4SXFlQ3p4VXlZNVlGeWRnRFpnYUJhRGtJdE5QdFI5VGx2WlVtb043ZG1VelVhV20xQkNaU2V6VzBRdFBVWG5BbEh1eHpURmdmekZHdkVPSFVtMjloYnQ5QkhQL2tSN0huMXRzd1BiY1ZEZmV3UW9RVkpVd05wckY5LzBFc25uMERCMjdhaDVzTzdNZkNaV0FSRG8wQTZNMWc5dWJiOE1ETnQ2R1p2NHEzWDNrQlgzbmlFZFJYeitQd3pZZHcvSTQ3OEo0N2orSHEvQUt1emM5allYRWgyS2lMNllkQWE0NTJWMm1ISWdWR3h3QTIxWld0dHQwU3hwTlFOZ1dNb2pRRFNMMEdDV0hpYklhTDJOaG1XUWVTUXFCbUJLYkYzQWtYSkxoRnltL1VzdkVwUzlMbTU0NFJPNEhIYWo2U0FsTkszWXAydkpHbzJHL3hIdG1TZ3VMZWhIY3FzdmUzV2hGdCthM2xRb2twYzkxYWFKU2k0NHFraTVDaS85eVlhaHJ1dWxqTWdlOFhpeVVvU3I4enlwdEhjd04xTk8xTzZxbFNXYUpHQldpcXBhRFRSckJQaXFPcE1yWnB4bFJyYSthdG9mRFdRbWV0eGp0Yldpb0JYR3VrdXNrc0dnMGRtTWxnVEx5RGJGTTNPSC8ySE56c0hHNS84SWZ4MGNPM1kyclRWZ3lwUWdQQ1NnaUdUaWw1K3pBUm5LdVN3MDhqUU4xWVFVMFB5d1RRN0Jic3ZQdjkyUGNEOTJIaDhnVzhmZUlGL01sWHZvWE5VNHk3NzdrSEtvcW1ia0tEa3FSN3o4YldIYTNySjlNMzBRMEtyWUE2NGI0cUpYMm53WU9pMENhTGtaUzRjUDFKdWIyOXcxSGdGaG9Kb2pXKzVmampQV09sWkpOUHhuT3ZGNmx5TWw0U1puNG1XVk96NkZRYzhUUE44bUFSR3h6TjNwSnMrcG9HMitRcG9DMEhwTGhMVXhHUjlUVlFWQ3ROYm0rMGdnR2lQR1F6M3FmQ2JEUFNkMEcxbE9iRmdRcHY4endVTVhxZTVlNDVHK1VqWHBENVQwcjFZN29KTWJXUHAzQmFQR3h1NUJoWnFmRTJMR0pMOGpLUVFuYWFHNUkwcXhYYmFxQWJxc3JHTEdMVnpyOU4ydnlyQlJyVmxzWStLc2N3QWxSeDRNNTc4TUIvL0ROWXBCNkd5bGpXbUhrWkFDazBtc1NKUHNRTWNweEFMVThIeHN6SXQ4OTZZMVhHa0J4b2JnZjIzN2NEeCs1N0FOLzVvOS9GNHVJU1hJL1JoTkZtQlE3U2NZY3lRWGJNMTlaQytYZS9Ka1lWU29sYWhnaUlnMXVTY2VyTms1K0M4bzYwUlJ0bnJUNmJycjVZcXJFcHpUZzBJRldVUjkyVFpjcGlPYTJscDdFOVpwSk1uZFNZNk1ZUTNRRGtjc2NyUlc4S1NpMzZLWHUzbG5ySXJzUHhBSldraGdsQmF5aU5qNWJwdERaQVNJZ2F5UnpVdXB0b1RydWl1VUtTejFKR3E3V3d6UEIrOVZ3NnN5V2hUbXFDME95MGFtZTVxV1k3MFJqcDFPRGthdHlJWTVTMkdJRnFwK3BQUktQWWpLV3RBRE8xQS8xTk5qOHdVY3d5cGxob2hheEp2blBoVk9QY05pM0I3NzgvdXdYejZHTWttY2d5dlNGNWlBcDVDemdSM3lISEZFMUZiVXBPaWY2eTl2UkVqSVlJUTBjWXpHNUNYZGRRZUVaQmpTVzNIZlM1MnYxWnErSlBUZVMwUnArRnU1UG04ZlN4YWFjS3U1S1JHN0ZTanp4UlFVRW1YWDdvdVkrekxwT3pEc2VUT25CUjRYdlMzK1AzYVNrNUYyMXB2Z3lMbExHcXR1QThNMGlheG1DYlRNZG82Qk5ZbnRhcUprQXdlV0lHWiszb1pWR3BKR3VQWEY4bnN3Wi9jemk0bHJRZlVrYlB6V2dzSXBDSXJ3RmpWNkZtRGxNN3kxMkxkdHBvL09EekhFMXRsMUFxSWw1Szg4bUlUZElVWDRNWUZ6SXhMaGdBRFE4UGFwMWtqSk5zYTZ6M2FycnlBc3d5bzUrMTVhSFFYdWU2cWlLT0RHWmhaaEdxRmwyUlRJUlJQY1NWeTVkeGRYNEJoejc0SXhoRmIwTnlXU2hsWjBzRVgzNEpMZC9XSE5QYmtSbi91RkFhSkNkbmlvTTJQSE16dTJVN252ck9YK0hRZ1gzWXNtRVdqVWkyMkZLN1liblZoN0g2eVg0alRYWWN4R0tGUnRGR2pkUGFEY05xQ01sZkljMmtJTnVBRlN6Q05jK2c1R0MxRmIrM0lrN2FHREpXM3hMYTZTbTlwN1NBUEJSekk5V01KeEE3Zmp3UFRFOHJWTzNBa05SZ1pkSjl0ZENsRmxpTGdqUDNyeVhpbEF4QnhGS0FtaWtFYTNmVXRMSmZ1NlFMSGozMnUydG9yNVhDSktkUTdVUmtXQW81VFBuL21vWmlXUHRPTFZSYmRzNER0VnA5dW1kcUJDNGxLL20wTzErdUlGSk5STlpWa3RJVUhLbFZhaGhycHc1TnBUb3h6eTg5NHBBekszTVhGQUtxK3REcE9TendOSTUvN0VIc3Z1VW9wcmZ1UmhNTExjME5zbm5sMmZHZUZCcE1takJMSUdBQ1FXZWdrcDJnbUdENk9Yd0dNUkxDZ2J2dng4NTlCM0h1dGUvamk0OCtnOFZSalljKzZNcDJYNVBXZGhwVFNRdVJqUlc1V0diS0t2N2FwQXFpdGo1Y3E0dkJrZlBJdTRqSVU1eGhHZjBBV3ZoSzVOUVJBa3dNSEd3bVhVa0lxZzZFQmswQXp6VzFmTGVIZDRoMUZsUXlROHV6N0VqVFpBa3BwZzFKbk5TVnN2TXNIQ0poNDBkZ3lna2QzMWltcmRTelVuV2xXQVB0N2pzVTQ3RUtGMDg3b1RUNU9FdVlkSkk5MUZKS3BnSDBDelJXbytPMlBSazFvcVFJYVZ0NGlWcVZzdWJoaTVyTUo5UjZGMlZ4YjFENGVhTUZId3lTdVNlcnQyamlyTWttN2JhVTJTYVZQQWkweFMxRHg2UzJKVWROMVBsS1dpU3BUTEw5QmhwTHJ2Q2RvbWo2Ni9CM2Z1Vi9nT3NOVUFmbk9TRTJaaTJhcUpYMG1aR3R2R085MnFnZjVsRTNOWWg2ZnM2Qm91ekNWSlNtbHVxeGcyVUZhT3N1N05tMkV3ZmU5eUdNUmlzWWNaV1ZvMGpPSENZbktsdTR5V2FDc0c2K0pxeW5HQ0hXVmlBNVFjUTVreFc3TVB6RjAzQWNUbi9IZVFOSDV5VXFsblFvQzhobVdKeUhtQmluSVM1bVdWaVFVczFhRElhcnlnYWY1dlQ4UXNGYkN1c01icUptQnFWU3laWkVJSytjUTJwOENhMCswblk1cXZGTEFGQnBrQnBwR2pUQVJZVFJURUdHK3BwYU9CdVp4Z2hUbzVvc2daUXo0cHU2NWxBTVNPQTRmVVZ0cE0vanV0U2tieWxlU3c1WUpQa21rUzBWcUoyV1o3Y2dDaVVLaDRXY05xdVlkSk5nUUMxYkJ1WEJFMkExR0I5bFZ5RTE1WXRxcDNQRk5FaW5UV0p0bjdMNnBOMDRTNG1IRmlKSWJ4ckRlQUpGbWE0WVZadWZlOVRpeDVFY2VtSkhtb3FmNGhOdldhQy9QVnNUZzJFVXpFUlB4cER1S3doRFZRd0JhRFVJQnBabTgwZUpyV3FMTWtVQnppVnpsVGpoS05UZmFrMDQ0andGamJXODMveCtHclY2bXl3WFcyOHBHV1JRVk9OUk5weUxCWDlpbE5pSUw2SjlsOW5ZUmREaWdLd0xtUkxaYTBlVW1yQSsyWnZxdG1ZVkpqTVk1SkhpbWtBOHlyMHNVV3B1NndiTkhTc3BzeUF6aFVoYjdjMWtXWTVZUVlSR0ludlR6WmJOM1hER0p0bDQ4NWdVbDRxNlE5SzBINnZVSW92VkZNS2RvbTBUZWJvdWtDTzBtbzFBYXFldTVsRmYvdi9DWnVNWUhhbXdDTFBPUG81RDYzRmtGY0lKeTF3S2hOUUtQNUJ2cUlwaysycmtkRkdWdTJtOUVWUWxLeWt6N3pURm04ZzdtejRCVFkrWkM5VWdrOFdQa2J4c05Td3dUaUtYRE5EQ1poeEVZWWlrM3pCYU4xaTRQbytWcFNHa2NTQnBrSDMwdEJobWt0OG40MFdGODQ2aC9TaUtyNVM2SFgreGpFbmo1eW41NmFWc044d3dJTFhETmpVZEh0RVQzNCtnaC9mQ0R5ZHM1Y0lnVE1yTk4yeDhLWmhhVkl3R0tUZGx6N28wemsxYndqTnptSldIakZHRVJyMTJqdGVtSGJxczMrTStGSVNaQ3pGQUk5T1JxcGIxeXYwWWRyMUlzUEtuQUc1YmlsVk1oaFh2ZjBYV1NxOUlzZFYwdlZIcW9VOExxcFF0bWJRbW8va3hNbXRLcFRWSFBpVnJkbDZjcXBTTUdtMkdJZGxPT2JBSmNmdzJNYWVnUkN3SnlBRWJPek9qeFk4S1F5Qkc4UmlZSmZjOWpPSDFyUkNEZ21VVnViSy9YUTN6a1kxTlRHaXRjcWtsUm1mUmFWZzEwNVdUN0poeXVsdFlaUUdvYkI5QkxvWUtKeDV5UGhXdFNOR0RvTDUrQlM4Ly8xMDgvdVV2b2pkY3dLdXZ2WXBubjNzR213OGNSWC9kRExidTJRL3RUV0ZFRHFJdWpWZXpRM1BJMkhKbDlXZjBmdFJFbitYTDQrQUxXRFp1UlIrL2VBL2lSaTEwdVdUbVB3Ui8veWdjaXk0Ny9uZHZ4RkVGcTYyVTdzZW1ManRxMURCYnVmTXlHN2hrUVQ0WkdxL3QzMkNuKzhaZ3g4WHN5TnloNkE4b2lSNkR5VkVxTUY4YWg4WUlsQVJOVU5heXNza1FyR21xNlI1STF0K2Fja3hxRFR5T0hZNWU5aXlvb3NwTnFXMk5ncGFjTXAvK3RqTTZaN3VFTXJrTXRacDJRZHpDZ3FsSXVWSFlRcGN1YmxTaXFyQjFjYWtUVVRLTlNTaUhTbXBZbEdJbTlVUnZ2YnpSTkErQmhKUzhmWUVEb0J6d1dIZ1d0UEVDTGE2L2JKQXhyYi9HaHRwK2I2THlWRHR3cGRyYXV0QzNtOFVDUU9vUitzMHl6cjkrQWk4Ky9paWUrYzQzVUVHd2JtcUF1YzBiY2VUd3JUaHl5ODE0N2JYWDhkS1gveGpQMHdEN2p0MkpuYmNjeGRTV0hSaWlGNERiRmpVc3huNE41R2NOS0NVYVZsdWRhM2JDRG1tbkk3ZzdBTWh3ODdrcjBuK3ZDKy9qd29RY0RvR2dpaVdDWnBOT0pWOE1jU0I5TlkxdE4yYWVCdHltZUZEa1VWRitqWVFUV2x1ekxVUkx3eGV4UnJEdGNYSmhRbFNXNTBiaGhhSDhoSTNFUFh4ZURwb0hpV3N0VG50V0F6b2JwK09veE9XWWNRUmNMUnhhRlJYdFVlM05QbzZjMFJJRExBeEdLQ2tJNDgxU1d3NkRPb1lObERDSWZGWVhtVVh4UHRKcUZTOHhUalU5MjNHamtISTVWVGJoZlMxNU1wa0luOWExckVtU29scGFsblMxQzJVd29FSVZhVHpsMURZR3Q4d3NxZlFyMERHd1lsWi9OUmt4NExpU0crREtPZnpKdi8xTnZQam9kekM3YmdZYlp6ZGdNRDNsK1c3bkFGV3NuNW5DOFR1UDRxNjdqdVBpcGN0NDZhV1g4Y1FmUFlMTmgyN0RiUi84WWRTRDlTQnlSY09Wa2hUWFE0b1VYRHZ0TFVvZDBVNEdyYm9tSFptT2poSno0eGFzMlpiOS8rSHN6Yjh0dTg3eTNHZk8xZXptN05NM1ZYV3FMNmxLS3BWNk9iYU1qWTBGaG9Cam1nQWhEWVNiM0pCQXhzMi9rWnVNT0Z5SW5RU0l3VTV3QnpheE1RNFlETGlURzRGa3lTNDFWVkwxZlhQYTNlKzkxcHJmL1dGMWM2NjlTODZJeG1BTVpFbW56amw3clRtLzVuMmZWOW1IUkY0aEk1VUVLbDNPUlhObWRXYWZUU1czcXRUcFc2RTJvaTNIZDJFT0t4cUpMTFRUWkRrRzVXZHN4UDE1Y2dkc2NWRVpaZTN3eTUyaWFHc0lYS1JMWjNXSHFSQlF4SDJ2Y28xRzdzakxBMGZ5ellJcStCd2V5b0JmN2hRcmd5dDcvMlRiMGx4UHArUExWNWJKUlJ4YnNaNUllSjFZS09iaUZIbFRoWTMxdFhFTUhjWHRLL1phUjFsS3cvSW1LTkZVMWpHVTV5Qlk4QTFWQllWV2ROaktFUXhYQUo4VC82VXRNWFpUN1JXMkhseVYxRi9yOWhSTHFGSUFTeTArc2ppVkdhWEdYUUVtWVhUbk9wLzZqWC9IN28ycnJPL2RTejBNeTVzbGl3RlBramdsME1RUlJzY3NMclQ0b1hlL2c3Y054M3p4UzEvbXBTOTlqc2ZmK3pORTRRdzIzMXhOSU5mSzc5Y1ZqWWwxM0xuTFZWdDlxYXFBbkt5OEZSSkxBWm0zVWFuQzBZaGdqTUxvbEdnbDltNGUxMERvRk8xV1lxKzliWExBbjFaSUxrYUt0Q0RIc1ZWY1FIbVdaTGJTRStNcS81U2xIaERsekw2SzF0aVFaajdhcnNSTTFVZ21VSElxNFdMOVdacmZkQjcwbzBvalV4RkxsNVM4Q2QvNWJVN1RuMVoreUtxSVIwM3N4eXZBaEdKMWRTOXRtNzJlVWxQMGNYbVpaWERTVzVRbGVsQUd4T1BOQVhNNWpydENqN0tDSTB5RjM1LzJmUlUxb1pNZ00zM1g2bFlJYXJvZkh0ZkdhVk9GY3lja3lsMHo1VkpxbDI4c2p2UmE1eUtmeklpaUpTSGF1c01uL3VPL3BYUDdHbXQ3MWxCYVoyYkpzbHMwOXRiQmdoRkc0NGd3OEhqLys5N0xGLzdYbi9QaVgzeVdSMzdrcDVCYWsxSXpKODdTMXA0SlZiMlh5dm9FMUpTS3ltVXNZbTJZTENpY1hiMWxDZExHZ05GQ2xITWFzaWd6c3BtQjNmT0wvYVd6LzE2Sm1zaUZLQU5pU3l1K08vQXM4WEEyTWs4cytiNG9OV245eXAySlVsR0NXdHhNc1lhTkJUVW5mejRTY1YyT25rSmJZUnY1V0UxYlEwWnRQNGRTcGpyNzd0TXIxZzFxVCtUZlJMZG1XMS90bDFYbGJQYnNZYlpGRFBjOGJhb2huZlpRVEU5V0Q2cnlpZHBKYXRhR0lUZFpLR2ZRNklJWHl0Zkhlb2p0MEVCSGludXZTcVZTRWRud0VWM2tpay80S3V6dGkyTXlGRFgxWUhIQUxCaFhCcHRQK0QyTlRpSms1emFmK3MxL1IzL3pObXQ3OTJZT05WV0V0aFE1cUNoSFA1NFMzdzJZdENyd1BKLzMvZGdQOC9rLy9YTmUrT0lmOGZpUC9YM2kraXhhYThURTd1KzdpQm16WmpneTJmNjl1U2JTdm5pTXBSUlZ4UTJzYzI2QU1SaXRpVXcyRE0wR2lMR2xPVUZLMzRyb2N2WlRLRTN0eGJYZ01pUHlpc1VDQTZpOHA3YVVjR0pIemRrWENXWEFhR0diejkyR1lndTBIT2Raa1JLY0M0bDBkdUFwclp3RFVtZE1qUkpuVUpLdUMxeWZ4VURRS3FzbUZOa1E4RTFzS0twNitkc0NqWHlxbi9jdzJ0M3RDaFVqemJSaWVxSzFjT1c3ZHY3YXRIdDJxbkxlWFN4VTZwUksxNndxUGJUelI4ajBCMVZLRkZVWkVqbVplSk0rRUZZYXJid1pCdGNhcWxYMnVPS01XTTFrUGVzd2tkTUh3amN4cXJQSnh6NzRIK2pldnNIZTliMkZNaytrak1ETXN4dTFOYzlQcCtZNnUyQk45bkRIYU9ESDMvc2UvdmgvL1FYZis5TG5lT3hIZjRhbzFpeUNLZ3BZaFMwZUttWXIrUUdYVERvZ2JOQkRVZGtabHh5UzRka0trYjFRcUVoeko2cEtWQnJqbHVBZzFaU3RuTXczMnJZZnBCSkZqM1hibDUrTXNlWXdLUWkzSUJRcGx6QW8xc0VoWXN1N3NDMnYxamtqRlhHYkZKa2I0Z3lSODBNZ2NSRGZ1YXF4NUhaa29CdVJ5VTFibmw2VkgwdS9jL3FPRklNRXU0K3NBaFR0azZDWU1MdnpPR1V0b1l3U1M4ZXVyS1JhVUZNczRpSlNTWWl4ZDc2bENVS1VtaURwVGE1aUtyZE0xa2hwYTZWcGNMWlhxUnZPR0NzVDBLTDd1bzJOODFCaDYvOTFLVTV5QkVtNXFVcnBlMWhkcmIyeERSTVZ1MU1XeTdCaWJIZEJXVTRMZUlHUE5nbTZzOFhIZitQL1plUGFKZGJXVnEwSWNHT0ZEeWxNRWpIcTl5RWVzYlk0ejVOUFBNcUpCeDlrWlcyVnNGWkx4NGxTN3ErUmhQRW80aysrK0NYMDJqRWUrOUdmSmc2YXhlVE4yRHc3UzRzaHhtclhZS0xTczFmTE9ZOUNsTExLWVhkL1hlaFN0RnZTMnZvVFZhM2FuRCt2ckJUdjZjb1VxeWkyTGo0cS8yMytQZHJyM01MQVl3K0RDeENvY1FiWGR0c2praUwzaXdQRDlvTlVXQ1VPMGNxcU9QUHZRWXVyMkMyTlJia3VSS0grMjh0M3hiM3M4aDlPVDZ5MDBwZFVUNlI2RlVNUlM5SnBselhsd0tXeXhwTUt1YVVNTUM5N1lpTnVzVEMxL0xaZm1vckhXOWs2NmF3RXMvaCt4UU5xaVhKMC9wS0pWZTViVWMvaVNJR3JrbjdsQ0tSTU1jRlZoWGpFT2Zrck9YMzV6MjRaQ0t4ZlQvN3dsTCtyOGpaTGZ3dWVFcnpPRnAvNjBIL2c3dVdMckt3dUZ5dEVyUldTeEl3R2ZmcTlQc1BPTHAyZFRZakh0R28rNjBzTEJKNmkwKy9UV3Q3RFE0ODl3WkhqeDFoZjM4L2MvRHllNzZjdnVOTDArMFArOUV0ZnBuSGdBVTQ5OHo0aXYxSEdwNnRTclpqYnlvc3pMaDk2S1ZlaVRUYmhGK1ZsMEFxdjhLOVB6SVdNcGVxME1ocExBMUs2cXF0cUw2dHU3bkwyN1FxVTdVYkdLT1ZJYW0xTFdaSFRvR1NxdmJQcTlDeEJ0YzVxeC9udlM1S3ZyUUxQMnc2cGhIdllRMlJWRGd3TGU0OXkwNDF6WXBiazYxQ0YrdkRMZHlYdlhhcURHOHUxTStXbHEyQ01tTGJ2cCtqSGtGSlZKcFZ5U2RrckR5V3VabHdtYjR1UzFETDVLODdVUUtVbVVTUlQ2NWpLM0YzSzRXNzJKQmhiY3lEaTNzNjJXcXpJTGJEYThQTHFLcWtOdHE2N2tHWEtQY1llcGN4VExEaEV2djRxWncvRzdSV0ZIRm1MbUJpL3Y4T25QL1FCYmx3NHk5NjF0WFJ2SDBkRXd3Rzl6ZzZkclUzR2d5NWFoTGxXay9uWldXWm5tc1NqSWVzcjg3enI3VSt5dmJQTGxadTNPWC9sS3JmdWJHRHcyWFBvUGg1NzhpbU9QM0EvYTN2M01OTnEwUitNK09KZmZwM3c0SEZPdmZQdk12UnJsb2RkckxCbFZTR2p1K3hFVzdxQ3RSOXd4N1hLTFJ1dGpVcXhzL0Z5dGFVNG4wWHhraXJMS0ZOcHJDcVR3ZkxQVnptQzNkcG0yRzJyc3Z4N3VlTXhlMmZjb1hNbGxzZCt2cXdUcXZoNVJGZGVRVzNOMGFwRFJUdUFVcHhudFZSbXlrVHpMQXJVaDErNUk5aVN4S3JkMHA1TEtHdjZyYW84ZGFtWXVNUlZ0VTBGYVZSdXZ5bnVlTEcraGgzMmdUaG5NWFpNVjM2N2E5RkY3MVpreWVQTTZTcjZlcHhFV3pLMnVuTEVSWldRUzZ2cXFQNkl5b0g3bGRWT1FSS2V3RUxnNUJBNGJqajdKelVXbXh4SjQ4NU5qTmZmNW85Lyt6YzUvOUx6TEM4dE1CNE02ZXh1a2d6NzZDU2lYdk1KbEdKcGNZSFpWZ3MvaCtwcjZMVTc3Rm1hNFVmZStSUW1pZFBrWWhFNi9RRWJkM2M1ZS80cWw2N2RJRmFLMXNJS0R6enlDSTg5OFNTTm1YbGVlUGtNQzhjZTRiNGZlSWF4RjZaQWk4TEVNc1ZJNVhUSjlueEFISUNHcXh1Mll1QnNxdENFeDFveFFSOHQxSW02T21EQmdjaGJWWXFMTHJmYjRzcVY1d3czcGJqb1NxS3pUT3FmY3p0T1B1QlU5andxaitwVEZwUTJtelVWZkVJbTgrU3MzMVBPQVZRMlJxMllZN2dQcWE4c2pxS2RwbHBKRmNadTRtMTdaNW0rYTAyelRTWEx5OG9qdGxWZ1ROZzczZVRDUEZmQXFIS0tuMDkweFpiYWluS1BDMlZQajkxWm5uTEdHWklWRXhYUlVYNGFLNG8xMHFSaldGdzlrdGlUYm5IM3hIbExZSEQ5QlBZbXdycFY4Z1JqKzRNVlc5NWJxV1U5RXhNTWQvbjhSLzRMcjN6OXJ6SHhrTjd0cXpSOHhWeXJ5ZExlUmVaYXN3ejZBMnFCb2xFTDA3eEhZK1V3WktHbVJhOGZSM2hLbUs5NXpPOWY1T2orRmM1ZDJzUE0vQkszN3R6bHlwbnY4dG1YL2diUk5XcHpTOHpkdkVFMDd2SElELzBFUFYzSDZPd210cDErK1U2YWN0NlJ3MTNMbjB1WG42bHpScXBLSDIyUFRwVXowQ25zVldJNUt0U1VEQWI3b0JCeGc1dEVLdGVVS3doMEZKN2lpcG1NTW1pVHR0SHBPNWNQODBwbHF0amJHeW0zR21KRjV1VUdXNU43SXdvRG5CUmJDWTIyK0xqNW9GQTc4bU1uSVVsSzhacFNnazlpRGQwSzI2bHlmZmRUTzI1cmFHTVBwY1UxOEpTdFR2bkRwaUVGMXRjdzFzMXJaS0lrMXZZYmJFVjY1YWV2UTE1UktZZkFUbkVWVlE3TTdIMnZzcWFpWmNwUWZ2dW95dW11SE9seHFaa1M1NWJQVDJpeHl3N3JZVk9WdFdRWldtWU5IeXNXV09jRU0zbDVtYUMxUnlBUnlkWk5Qdit4RC9QaVgvd3hBY0x5d2h6enN5dk0xR3NFZmpyTmI3YzcxRUtmUmhoa3ZMN0tlazN5T0d0S3NHc0JtRlJvbFhEa3dCcVhydC9pTFk4ZTQyMlBuNkRUSDNQcjdqWVhMMS9qenJrWCtjNkZsOW05ZVlOMy91d3ZNbXJPRTJWVlJPRjRGYnQ2dzdyeDdlcks3WWNuaG5MT0dleFdBaXFMWTAwdEFtTEpqZFZFbTFybE5pckx3RjhTQnEyRGkybEJZbFVadVBWWlpiTVA4Y3BuWDltR1BrdHVScTVNclJRWGhZa3BQMFFxNDNIdGJJaXNvbEVaaThHSVkrcktaWTdGRVBCM1Q5OFZXd0dsRlc4cWdIVVVaNElUd1hVUHJFVjV5MWx0Z3NNeHNUTG9GWGFNNUwzMnhHLytKNnJ2KzkxOG4zOWZXUkxmcWtiS2lPczdzTjlUcFJ4NUw1Wm9SVlhXZnk0SVU1d1VFeHNXS3RZNlN5eGlqVWhFaTVoenozK0RUMy9vQTR6dTNtVHY4Z0tyeS9QNDJ1TE9HV0ZycDBNOURKbWJhNmJZQ2V2M25hK1IydTAycXdzenZQY0huOFRFNDJ6dWtPazVzdjdUQUwzaG1PczNObmo0NUFOb0w5WGZHOUYwZTJQT3ZIR0IxODVkSmp4d2t2Zjg0cTh5Zit3aCt1STdRK0dpTkhVY0RHSzFTOStmRGxRa1dqdk5yRGd2QUZsVWZaSHhMdmtMTTlsYVZUYmNHV3UvWWpBcDkxRDNnTU80MDdOaWQ1TWZRRlpiVWMweHVMZnNmdkpKTG9va21jeFdGU3owdWFvKzlSb1hvcGNkUmgvKzN0MmlJcCtVNlNpTDcxWmRwOXhMclNzVmM0NnlYcFRLeTJBYlkvSmRxa2c1ZVJVcEZFemxpVnYyTlRaVnR0eGY1enA4NHc2T3NxOXBSTnpKdlZMM2tLVlVhWU5NM0ZCTWlHRExHY1UweHAyeStrZmJUbTBEUU1TdXdDUmwvaFdyUHExVC9ud3lKdHE2eVZjLy9URysrZG1QTVZjUE9ISmdIL1V3U0xtQXFnU25ibTYzOFpWaVlhNkY1NVZSMU1WQm5MbjJkcmQzMmJjOHh6UHZmQUtKUnlWWFVldGlIWm91K1R6YU16UFlod0FBSUFCSlJFRlV2UkhiT3gxTzNIOGtpd1RMRGlaUGMvWG1KdC84bTVkb1J6Nm5mdUlmY2VxWjk4SHNDbEV1NGhHM2hzeEpPblpLazdKWGRwVituQ2tLRUZ0REpaVVdTU3I3WjVYZDcxWWo0cndteHJtSmJSZXBWTkJkNHVnWnkyOVRwZ25kbldkUUprYlk5MDYyVmxWRnFkUDBWTmVhMmIrZDhTbnlvSjFjOERTUmg2SkFmZmg3RzZLVVRJemtwSWhMbWl4M2xNWFJ1N2RGaHFrM29EUDJzejljRzd0dGFhMGR0OWc5R2hKN0ZWSnVWcVFNRTdINTlOamlHdXZ3bVlhaGN2emJybkZENlJMYlBQbDhXcDV0YTZPbnJQMjdXTHRqeWN4STlzMGpscVMzbU9hS29TNWpicnp5QW4vd29ROHd1bm1KbFlVVys5WlcwMW1KS1EvcnhNRFdUZ2RCc2RCcTRxV0pKbmdaS0NNMmh1RjR6S0RYeHlScFFOeUJ2Y3NjT2JoR3A5TmhaV21CeGJrVzlVWXRLNndOV252cDN0bnoyR3IzNlhXN0hEdDZDQ1ZKc2ZJVHJla05JNTcvenF0Y3ZuNlg4TkJEdlBzZi93dGFoeDlrcU1LaWJDMCtTOEc1SFhNa1dkVkVWYjJsUmRMa0txbW9WbTBOU0U2L0ZVZnlMYWczeVNlUTZtREgrWHlWVzdWWVg4bUlYZmtaUzhoRDFwTkw0YUYwenJXczdTeHQxT1ZXSTJkRHBlMmVkbDVpbHg2b2lzcEZLNDNXNWN6Q21ESkV0WnhYVzUvQmgwOXZTajZBY2ZweVIzZXRDa2xpdVZ1ZkZIR0lvMFlxU3luYjRESmhyVlVsc0xEYWk2WGZxTEVNTnJhQzBDMmljcHB4WHE2SzJFSUtNMTIrYTRzazdLL3Y2RWJFMnJmLzc4RXJIZTQvWmJpSWtGakNEVlZsL0ZnUHN5bTgvZmxENnl0QmQ3YjU2bWMveWRjKzgxSHFrbkQ0d0Y3bVd6T1ozaUFwaU1waUZMdnRIbkVpek0zT0ZMdVNLRTRZRGdkRW94RmFLMnIxa0psbWc4QVA2ZlY2TExacXZQdnB4N2g2N1JxZGRvL3VZSVR5Rkdzcnl5d3R6ckV3MTZJVytpazUyUE80ZVdzREVlSEEraHBLVEJhK3FRcm0zWVhMTjNudXBWZm9xZ2FuZnZ3WGVPSkgzcyt3Tm90UmZ2a3EyYVJjd2ZHeGk3RXZuYktDdzM2aFZXbW95ZVc1VW9UU2xtR3l4dDc3VjRRM0ZIWmdYZW9GYlh5NG9tQmFpRDFBRmwxSTNzV0s5YmF6UzRvRExYOEh0Q28xLzg2YTJENmd5dXJJWklPOEFncXUwdm1QV0JvTFZ5MlkwNHJGcVU3eW4xSEVCWkdyRDUvT0t3QTNLQ0ZmL3p1WHRYM25Pdm5vMXQ4N2cxVnhiTGhPSzJEYmMrOVI1WW5USDVjVW1PdzNXWUkyTW5SVFh0N25tNDBjZDIwY3RaV3lpQ3o1R2ozZkhWdTRzeHhvcVpVenhNdS9Wclgxa0luSWE3SG13bEpDSGJIeUZvcDVzRFZ4emc0OWxla050TkxVWk1UZE4wN3pCeC84ajJ5ZWY0WGx1UmtPcmUvQjg2UzBrbG9kWHJ2ZEo0NE56VWFENFdqSWVEZ21pdU5VRGFsaGVXR2VNRXhUaE5MS3c3RGI2VERYcVBHKzk3d1ZFNCtKb29UQmFFeHZPS1RiSGREdWRobU94aXd2TGJHOHZNRFMvRHpOWm92TDEyNm1nYUlyODBCY1RwaXpuT3BPYjh5eno1L205bWFQeHRGSCtPRmYrbGMwRHh4bnJJTkNGVnRjRWlMdTczRkM1bEcrTkU3RlZOQ2V5bzJKQWl2bklxUHRXRmtTbnRhWUpDazhES0lrY3c5bVFiWGlhazB5WlhTbXVYZk5TbHBabmt4N1NGMEVmQmkzZmJibm9DWkYweFd4Y0ZrN1pKQnNscU9MYXNmWUNVbVZiWjNKbytueUZTTkp5Z3dvd2xIS3paWEpISWVpVERvRWRQZWFXQUdET1JMYUZVRm82NU14WWd1eVZLV3NLbGNkK1drR1UwQ0Z2SG1hRGxaMU1lMytkUWk4bFVucHZaRGVSU3VpU2thNkhSMWxSQ2JtR0pOOTZyMy9Na1ZRYWZxbmVVcG5aWjZwMkdGVmRuT3JvbFJJZno4SlBvSS83UEx0TC93UmYvclJEOUVrNGREK1BTd3Z6RG5weWJtamV6UWUwK3VQNkhaNjFHcHB1VjBMZlFJL1FHbVAzWGFINWVVNWZLMGNlSXFJMEc1M21aMnA4ZjVuM29xS2h3VmNWV3NOeXNPZ3VITGpEc1BobU1EWDlIcnBJVE16TzBlbjNlYUJFOGRZbUsyblpONDhXazdyekJIbjhlcTVxN3p3eWh0RTRTdy84QTkraGVQditCRUdYbzFFdkVLaUxkWm13bDYxdVRKeDY2R1hrbWVZMFdhTDI5Q3JRRlhsSGtQZzZ2aXViSWRWaFhaVnBsZFg1WVE1Wk5TT0tIZlNOdk1xd0VxRnRxRTRSdExuQXl1d1hESVBCaGFTVGFaOHo3cUF4S1I4UjVPeEFGU2hValFXRWN1cTBuT3d5a2RlM25EajJ5eGliSTVmc2xzRHNmcDVsUVVWR0VkVFZWVndWZFNEVnFsaVF6N2ZkSDVmb2VDNFFZNldJTTlwTDVRcjdySU9uaHhIbnFQSlJTYVI0cmszVzRwRUZ1dEJuTjc0VDNqKzdSV2xxcTZQTW4rQUZGL2RRM25wZU1wVFFzdURyZk92OHNrUGZvQnJwNTlqc2RuZzBQNjkxRUt2MEwrSkNNUHhpRzV2eUtBL1JDdEZ2VmFqWHZPcEJ3Ris0QlV2MGQzTkhXYm5aZ2dDM3gwdVpRL3JicWZIWERQZ0o1OTVHcEorUVNoS3NWdnAxOUYrd09uWDNtRGZuajNNTmhzTStqMTJlejEyMnozYTdTNklZWEY1Z1Qwckt5ek90Nmo1UGlKeDlwSjZiSGFHZk92NWwya1BZbFllZVFmdi92bGZ4bDg5eE5CNHhBNlp1Z3J5RUNkSk9GOEJpckxGYWZsbnBNc0xTZTd0TloyV0hLRXNjVisrSjA4a3QxamowSnBzUVpvZFFGUE1HeXl5VUE3bG9BallzNDBvbWhJZ3BoeWFiM2tJTXNYL1VtNUJwRHBRTExiU3lqblFWR1h1SlFMcW85WUJrS2YvT2ZEUEFyUWh4WXRlaXBIc21LUHF0Rjg3WVJpMm1yaGFBVXpUL3ptck1tY0lvTDdQSXRBOTZWWHhhN1p4RkdYLzZRUTQ1TDlzeGRTRjRmZlpmNVFIbklWQlYwcWNyWWNvWEptcnN0WTBXb0VaTXlzeDMvM3luL0ZISC9vQTNxakQrcDRWbGhmbkVBTlJZaGdNaDNTN2ZTU0o4WU9BZXIxT1BmQUpmQi9QVTluQXJ6UldiVzd0MEd6T0VBYWVFOU50d3luYjNUNXpEWS8zUC9OV0pCcWlQVC85M3JXWGNmdzBTbXRpQXkrLytob1BQM2lDZXBoQ0owZFJUTDgzWUxmVFpidmRwdE1kRWtjeDgvTno3RmxiWW5saGprYXpsclpCS3VETStjdWNlZU15NC9vaVQvLzgvODNodDd5VE5tR3FhTXpGTEFJbWs5VXFaYkVQckdEWTRpYXpmUG1veVhoSlo2a3M1VkNzVkhCTEdkQ2h5dWRDNHdKVmplVllWQlBQb1V3OGdGSkFWQ2xSUmM0dzBRMEFVeGJXbTRudXg2WnE1OFhpNUNMUWZ0THRxMFpWRmFmNVAvL0l5eHZ5L2JicTVlbVlCMm1VVkJSbCtiNk43WVJDS25NRCtkOHFuU2ZYSUpOMEhIRFIwaTVUZnByRXVISXMyRDNZUFVnMldLNUNldzBrdGozYU9zaktVN1FzRVhNMGxxcTRDY1hxMUNRTHJ3QUlTZWhjUDgvblAvSmJYSGp1eXpRRGowUHIrL0EwZEh0OXV0MCtCcUZlcnhjaUg5L3owOVdlS2oramdteXROTHVkTHA3bk1kTnNrSmg4V0pnRnQyaE5ZaEtHb3hHZGRwZDl5L084OTUyUEl5YW0wYWhuREgzdG9CZVUxZ3lHTVcrODhRWVBuenhCNEJsTVlyS2dWME9jQ1AzQmtHNXZ5RTYzUjY4M29EOGNVRzgwMkwrK2x6M0x5OHpOejdHMTNlVzU1MTlpcXgreDl2aTdlTWZQL0NKcWNTOEQ0MWt2U0o0eFVXNXpiT3VGTGFBcDNYS1o0MUwwcEtPejFDUTVCQ0s3ZmRSVmgwRWhTdE5GK1YrbVhtb0w2UzdPZFZ3S2svS0tNcnZ0RlU0NHA3S2xzS2pxbDVrUXFGSzUzRlFXRTZlcXovS0VPbWo2QmFZK21oMEFFMFg3bElyY0xaY0tSTUdrcmw1Y3I0UTRxMFZ0a2JyTEVaaWF3RUdweWc4cmJ5b0NLclgweHFXK0ZPa3EwMWMvOTBxbHRUY2d5aUt2S2lrcCszWTNJRElkMWFtbkhFTzJHQ1BkZ2NmVWt5Rm52LzBWL3VpM2ZnTjYyOHpVUW1aYkxhSnhPc0R6Z0pXVnhleGwxMFVLVGlHZ3l1S3Y4bS9OVTVydVlFUVN4OHpQdHpER2tJZ21NakdqVVV4L09LTGY3eFBITWJWYURTMndkMldPSng2Nm40c1h6dE5zTlZsWldXSmxjWUhXVERQRlJ5dEpjd1NVWnJmZDU4cTFhNXc4ZmdRbHNZVW9zMWR4aWxFVU1Zb1Nlb014blU2WDNkME92aCt5dHJaTWEyNmVjeGV1Y3YzMkJsRnpoWGYvazE5bHo4TnZvYS9xeFd4SlNkV2hKeTVZTE5QTGxzbzMrd20xQ2lHbWwyOVNkV05ZRVdaaWxlWk10QTFTR0xlVWt4MHBscksrWWttdk1LQW0zMVExd1owUWg0WXRwVk1VVlFHb3lqMmV4VGQvb2RYdm5kNFFKelFETndxcnZMTjA1ZVYzczh5VXBlS2RXUGZsYXhEcmVNdlhMa1V5aW5XaU90K3FzbGVTbGRKSFZZUkxVcUVZU1ZsNkNaWmNWMVZTam0zV3U4aWtVdEN5SjAreGVSU1BtajBCRmtjOWxwZVB4b2t0VndnMUROSGRxL3p4Ui80cnIzL3J5K2hrek9Kc2kvbldETFZheUhnY3NiUGJabjNQQ2xwTHNlOHRQUVZsWUVjeDdFVXpqbUtHd3hFTDg3TkVTY0pnT0tiYkc5RHA5dkFEbjFxdFJxTVdFQVFlUVJEUTZ3NW9OVHgrN0YxdnBkdmVZYnZUWlhlM3k2QS9RQ2xZWGxsbVpYbUpwWVVXelZxSVVuRHR4aDEyZG5aNDROaCtVSEd4dGJEdXAxSk1wQU1TTVl3ajRkekZxeGdEZnMxamUydUhuZDB1L1dIRVdJY2NmZWRQOEpiMy9RTEo3QXFSOGdwSmR3RUhSU2E5RzFUQm5YWUFtYXNxVmZkNk5aUXF0akhLQ3VFb3kvK3luVENTQXplTUt3ZVdjZ0JjRVpBN3JhQlVzTzIyTnI4Y0hPb1M3MWFwZUV2VWprSlhibjVWWGRuaHZoTkszSmc5OWJ1bk55VXZjNFRxUHI3c1pkMU9QM3VZSzFTZjB2ZFBDY1pVS3RXZVoyczd5ZmVuR1RKY1RSa0NpSjJLTWcwUko3YVQzbkVUWmYrYnpxYWZVcjRZWWhWQjFyQ3pDS1Z3aGticHhGYm5Lem1MWTJHS1ZGa3BSRTZTdHljV0ZDWFhEZVNKNW1rL21CUS9aeGg0QlBHSUMzLzdMSi8rNEwvSDdOeGhydFZrei9JeXRkQkRLYUhiN2JQVDJXWGZuajFvS2RlRjlrYWtQS0RTQTlwVFB1TTRabk56bXpBTUdZK0hXYVpjUUJUSExDN09Fbm8rMmxQNHZsZk1abloydXJRYVBqLzV3MCtqSkFhdGlXTVlEa1pzNzNhNWRPVTZ6WmtHdzBHWFpyUEJ3c0k4aXd2ejNOM1lvaGw2SEZ4ZlFhdHMyNUgvSHJSMlY2c29sUFl3K0p4KzVTd0xpNHVzTFM4d0hBeloybWx6L3ZKMXRyc0Q5UEloM3ZWUGZvM1ZCeDVuNk5lSW9sSmZYMEk3ZGZhaGxBVjVhWnNvc3dadHhMcmRob3J6Y21ZN2UrdmlVQmFqME9RVGN5ZGYwYnFKcy9aSEtWVXB4VzBjbHhRT1AyTkpjZ3ZKa3BRVzlmUzY4RW9kb2JhQ1NWUk9JOHE0RlZheWxOamlKRHRpWEpreWtzL1pJU2pVNzcyeUthVkVWaFVuanJKZTVOS1BYbkVINW9tOGR2U1ZOaTVYc0ZMcEZGQUlxK1F2ZHV0WWZEcGJrS1J3dGdkbGNWWldHN21xU215NnE1YU1uMzRQZmJXMVE5ZUY2TWc2d0xTdFdNdGVkbVVOKzRwZnRMR0t1NnhmTkpucEtVbjN2TVVLeVJnQ1NVaTJiL0dYZi9qN1BQK0ZUelBqSy9idFdXR3UxVXpqcUxSaXR6ZWsxK215ZDg5S1lRV1ZnaUZmOXFRS2pSRVlSakhEd1loQnJ3OUFyUlpTQzMzcVlRMC85Tm5lYWJPNE1JL3Y1d0l0NWF3NWQ5c2RXdldBOXovek5wUkV4YlJhbzBIN1hMNStDKzM3ckMzUHM3MjF6Y2JtTm5lM3RobVBJNVNDZ3djUHNHZGxucm1aQm1FWUZKbURFNlEwNWFHVlJ5ekM2VmRlWjJhbXliR0RlNHVzaHNzMzczTDYxWE1NcUhIaW1aL21rUi81S1lhTlJZem5POUhvU3JUYlZCVnV5NUxTTE5hSzJuN0czTEZaRHZQVWJpT3JLdCs0cmx5c3habGtoN0NxTWtYTFdndm1MdHppQ1ZFbDZ0ek9seWhSYzY3ektQMTV5eENVdkEwb3JkV0tRdnVMY3EzNGxpbE5GVzI0VjBTdHFZKzhzaW5WVWtHUkIyamFsbDdKdmdFMUFUY29YdENjUTViOW1hYUk0QzZscndYKzJscXppdGo5RjVoY0FhWXNhN0ZqZ0pBczI2WHMwOHV2VThhR0YrL2pWS2h4UmVHa3lqSlQyVndCS3dpenlBb1VXNVpzK1NVS1BuNjJ4eTEyc2NXL3lRd3hWNzczdDN6c04vNHQvWnVYbUcvVU9iQnZsY0QzQy9sMXV6ZWcxeHR5WU45YUlXWTF4VDQ4dlZYSGNjSndORTZIYklNQldudUVRVUFqOUdrMDZ0UUNIei8wMEdqdWJtd3pOOWNrQ0lNMGIwL3ltNkVzMFR1ZEhyTjFuNTk0NW0wUWo4bytQaGU1ZUQ0dnYvbzZody9zWTdaWkt3Wit2ZjZRN1hhSHJhMWQydDB1MnZOWVdWbGkzOTVWbHVablUveTRTRll1MjRlbklUR0swNjlmWXFaZTQ3N0QrOURLb0R4TmI1anczZE52Y0h1cmpiZnZPTy82aC8rS2hXTVAwUlVQSXlxTGRNOE9lMldaekVTRDVlWERxWk1zNGM3RXpFdXNwa3lLRnJXTTEzWm5VL21GbzdQTXlqeXl2WUNwSW5oWkFLMERoTEg1QmRxMmQrTWc0U2VJR0dvS2pUb1BVSjNhcjBxNW9peTRBbVhRclMyUlZ4KzFEZ0FxSGd5cTlKQjhONTRyN3F5cHFoUGRYQXhJckw4M3lsbWZHTnR2WVNpaW13cFpybUxDTGVpMmdIYmlDbVZXbTAzY01UYTJ6SkljVjlGa3pxaXk5Q0tJczc0cmNVNmxxbEhoL21FVjVsUVJhcW53SkNib2J2RlhuLzQ0My9yY0p3bk1tRDJyUzh6Tk52SHlQMHQ1Ykd6dE1oNk4yYjl2VDVGQWcwclZhYU54Ukw4L3BOZnZreVFKdFRBa0REeHFZUjNQOS9BREh6K0w0c3BCblJ1Yk84dzBHOHpPTkxJTmdPc2tVMXJqZVQ3dGRvZTVlc0NQditldEpGR1V5cGF4RUhCS0VjWHc0dmRlNXRHSGpsUHo3ZGxJT2gySkRQU0hFVnM3dTJ4dWJ6RVlESmx0enJCbmJZV2xwUVZtVzluUG1rVEZRRGdXeFN0bkx6RXowK1Q0a1gxb25VTkRBeTVkdThYcDE5NWdvSnFjZk8vUGMvS0gvaTZEMml6R0M3S0xVcWVGc2lYN0x2WUg0aHF3SnZNYzFEUWt5VVRnbDVGSzFIZlZDZzhPVEtTVThwU1llWGNCbHcweDg3eS9mRjlnazRFbnZwZTBtblZTSll6MTltY1NYKzA2WThwaVgweUJNQ3RUcjdORDRiKy9zaW5HK2hpTkJkWVVOVVhLYXgxUEpSRmVLc1JnS2FPUlZLYkx0c3IycWNZZWU5ZGZUUVd5ZnJHdTJsWWNRRnY1Q3lYandna1R3SmdwR2dkbnhGS3c1RXRFdEZRZmwrSUEwclpQMlBxWVZjR28wd3JxTXVicTZSZjQxRy8rZTRhM0xqTVQrdXpmczBJUWVLbkhBa0I1M05uY0JxVllYVjVFeEJESHFSeTNQK2d6R280SmZKOWFXS05XOHduRGRPL3ZleFlKeVE1WUZjM205ZzYrcjFsZVhIQ29MNklVY1dLSW80VCtZRWcwSGlGeHdzRzlLengrOGhqWGJ0eGdZWEdCcGNVNW1zMEduaVlMQzRXZC9vZ0xGeTd6MkVQMzRTbUQxbjYyRnM0cUNxVlIyaU5CMGUwUDZYUUhiRzFzc3JHMWhSOEVMQzB0c0x3d3o4SmNrMFl0eUdTNEh1Y3ZYNmZacUhQazRKN015NUVBbW5aL3pJdW5YMmRqdDQ5LzRBSGUrUS8rT2EzREp4a1JwdkZqWWdwQ2NLRVV6T2NsOWdPUG01dm5PTCtWS2duRHR0REkyZytxck9wdzBxL0ZOU3JaVkdRcExqSnJkRmZKR3l3d1lvNnRYa3JLZE1XOEpQbHpWcGlDdEhNNUZtdGx5cmtGRTg1VU45MWJmZVRWamRTTFpOdlZjditNcmt6WHErdUxmRTBtUmVLWWxSTW8xbXJJVU0zOHNMOW04VWNiNWZEV3BKQ0ZadTQ2NVlJazNBUVhLOVpjVktZSVN4enlUbDZ1TzY1ektRTkM3ZWl0YWRxRS9Gc3N2MzhiKzV3WlFqTDNuaVpoSnZDUjlqWmYrZXduK1BLbmZvOEdDWHRXRmxtY244MDJBbm44TnR6WjNNRUl6TGFhOVBzRGV2MCtZb1I2TGFUUnFGTVBhOVJEamE4OXRPZWxXbThwVVUrMkdRVThOcmQzUVdCNWFaN2NmeEpGRWYzaGdNRm9UQnduTk1NYTlYcjZmOGxveEVLcndUdmYrakNYcmx4aHA5T24yKzJSSkRGejgvT3NMTTJ6dERESFRMUEJ4bmFiUWEvSHNZTnJhVHZtV2ZGcnl2WTA2T0sxMjlydGNldnVOak90R1RZMjdyRGIzcVZlcTdOM2JZVzlxeXUwNWxyY3ZyTkZFSGlzcnkxaGtoaVJORzhnTVpwekY2NXg5c0pWaGw2VHgzN3luL0xBTzMrTXZ0Y2tjU3dzcVJGTVo3OWJsenhwTFdldEFJMkMxV2hSaEticVVteW9VTjZMWjZZbnI4REJtVUl4bURzQWxYYWw2Z1d6VUZzQk1OWG9jU3FTUmZ1d3FOaVBEZUtBUDdWRm5pNENSN01JZDZWc0hVemFIcWlQdkhhM1hGNVpiaVluRkZRczRKSjkzRkhoRWFJS0hZekNUU2N0aGhXWjhjWk9GYy83bFJJYk5aa01nNGdUanVsSTlLdXlIMFVSKzEzOWhZb3pnRlFWMm95VVNTeXExQlNBcXFpeHhFbGJMZFJpaGRrM29TRXhkOCsrektmKzg2K3plZjRWRmx2cHcxN3pkZkZ6SlFKeG5IQjNZNHR1djA4WSt2aGVRTDNXWURRYXNydzRUeGhvZkM5SUxaNlpqOEJZdjFFUk4xa1BGTnM3WFFiamlPV0ZCZnFEUG9QQmdDaUtDR3MralhxZGVoaFNDd0pDM3l0Q1JQcmRIclBOR2ovNjdyY3c2SGZvRDhZTWhpUDYvUkc5NFpCMnQwOS9NQUNFNWVVVk91MDJ4NDhkWXQvcUlwNlh4MCs1ZkpPaVBETHBKM1RqemhaaFdHZDFhWlk0aVduM0I3VGJYYlozMm96R1E1YVc5aERIRWZjZFBjRDhUQ085MFV5Q3FQU0EyMmtQK082cjU5aG9EMWs0K1ZiZS9yTy9UTEIyaUJHK1U2Y3JteU9aLy9tNnVqNHRuN1B5emxHdW9LZmkxdGVXK2tVc29sbFpiRWc1VEpTcTlaMkNrVmdNcmNWRmRhcDdVR3Vxa25hVkljSW0wcWVxVGx5cHJna25vU1RxbzJjMnhDNTV5aFVjbFlBSEpqMzh0dUJRN2lFMnFITDFLZ0RJMHBpZ1hDYUEzYlVwTzJUQlZuQzVra3dYK0ZrbCt6cXp4RktiTGFYQXh5RWVLZXNsSTJPc1l4OUE3akJVWmIyK1ZvSS82dkNkdi9nVHZ2QjdIOElmajFoZlcyWmhiZ1kwbUVRWWpTTTZ2UjY5L2hCZmE4TEFwMTRQQ1dzaG52TFozdDVsYVdtQndMZlNYNVZ0dUxKc29GblJsd2lNb3BodXI4LzIxZzZlNzZNVTFJS1FXaTBvNWdXQm4wSkY4b00yLzVxRC9wQm0zZWZIM3ZVVXlhaWJFUURTRXo0MlpOOXpuOTFPaitGZ1JCVEhqRVlqZk45ano5b0txNnZMTE13MHkyR21DRUpTU3FPTndZamk0dFZickswdE16OVR6M1FMQ3JSUG5DUnB1N0M5eTliMk5vSmlaV21SNWFWWjVtWm5xUG1wVERnU3hlc1hydkw2K2F2RXpXWGU4dE8velBHbjMwTmJoU1RvZERzZ0NhTGRTWGtWZUl1MS9yUDFycTdqM3czdHFhNmtCU2JBby9ZWFZaWmtub0tHWllmdWxLMnoyR1N0S3B1ZzBqSzdNN0ZKdVh5WnBxeXNkMCtjakk3aUxmbm8yUTFuNjVGVFovT1R6RlF5UWNxZjFkWUVTSmxpVTVnanJOdllhcmdxS3dmWDBPUElLQ3Y5djNOaVYwSXdxMUFScGJoblBXV3AxT3dwcHBJSmNLc3pBTlZTa0tZbVRDVmdVTVpRVjBMNzJuays4enNmNVBwTDM4NzI0M3NCUTdjM29EOFlFa2RqZk0ralhnOXBOQnJwdE41TGY5ZFJiTGk3dWNQeThnS2g1eFdqMGlKMFFxbGluU1JvNHNTa2x0MWVqLzV3aUZLYWVxMUd6ZmNKd3dEUDAvaWVSbXRkck9YRVdvZWxHb1gwNjNaN2ZacTFnQjkvMTVOSTFDOW1RT0xNMVZWSy9FVXhIbzNvOVVkMHNxSGtZTmduaVJKYXJWbFdNMzVBczE1TGUva2t5WjROUTJMZ3dwVWJIRHR5a0diTkorZlVTVGJTUzVOc0ZmM0JtSzJ0Ylc3ZHZVdTMyMk8yTmNPK1BXdXNMaS9RYXJYWTJHenp0eStjcG85bTlkUVA4UFRmL3lXUytUMGtYdWptRTZqSkYwcFZzVnlxbW9ucjh2bmtuZ0xVTWw0YzVkS014RXJ0TFcvOHlvMWVCUXNyNVZLT1ZVVTNLSlVnVmxYUmhGVFg5Y2IxUExqaEpxa29UWDMwekYxUlRsUzk1Vm11L21LY3YxYzRBRkhsWnRvcmExSW9VamtJbUlKZmxpa21BRnlibG5MWWVhN1lZb3BmY2lMVFZFdHUwc1hWajl2VFdsVVpFRGtsL3FTTTB3c0RrdkdRWmpMa3hiLytNejc3Ty84SkJoMUNyWmlmYlRFYURqRkpUTDFXbzlsSSsrMHdDQWc5VFFxTlRYdXhVWnh3Kys0bXl5dkxoTDR1ZVFLaXNnTXQ3UmVqT0dJNEdOTHREMG1Nb1phWDg2RlBHSGg0eFF0djIxTXpxSXQxT0tmaG1YbGJwK24yK3RRQ2o3LzN6TnNnSHFTQ2xTUXVoOURLd2xGNVhqRzRGRkdNeG1PNmd3R2Q3b0J1dDg5Z01LUS9HSUpTckN3dHNiZ3d6OEo4aTNyb0ZXdmFTMWV1Y2Y5OVJ3aTBaTE1hbFVhNXEzS2dhVXdhWHpHTWhaMmRMaHNiVyt6czdsQnYxRmxiWGFNNU04dmxLMWZaMnUwU05aZDQyOC8vYy9ZKzlEWjZ1Z2FCajRsakc4bzFTUXdYeC9HYjNnZTZqSjF3c0hCV05hRXFJVGRpelFiUy9wOXlDOGJrOHpRUlhXUTVERzA2dDdGdmQxV0c0a29WVHlmM3dIR3B5c0JlVFZiMjZyKy90aUZPa3A4UnhGTldnb3VhUExhc25EdVZBU1Z5NTVQTzg4ZXpja3ZqeEM0VzlrbFRBQk9sbUFuWWNzZnFBWkJQU2ozU2RPUE1DV0pWSS9ud3paNkh1cXNVTDZmdzZCSlBiVWRaRmVXYU5lZFFGcUJUb1Vnc1piaFdHbDhiaGpldjhJV1Avalpudi9sWCtKSVFlaDYxTUNBTUE1Sm96UExTQXI2dkMveDJyam5JWVExeFpOalkybUpsWlFXdGxZWG5UbWNFM2NHQVhuOUFOSXFvQlFHTlpvTWtqcGlibXlVTS9NejlOLzFoa09vVFliSkpzZWVobFNJUllUU0syTm50WUpLSXQvK2RSOWkzTk1kTW80NG1CcE5KZlBPZU92dWViYUJwdW9mM01nU1ZZVEJJTGNybkwxMm5WcXRqakdFMEdoS0VQaXZMaTZ5c0xOQnF0TGgxK3paSEQrOUhxYVJJM1UxZHA2YVVyUlpjeEZSWkdDWEN5MmZPc2JLeVJudDNsMTYvejNBNEpFNmdqOGZlSjU3aHFaLzRlZUw1TmFLQ0VpeHBsb0p5TDR4VXM2WXFOV0pWRnE2c3c3K2MyTnVabnNYTG00Mk0wcUZyaVZ3dm5udFVOak9Rb3JXZFhGSmFjN0E4UDhJaUJSV01CQWNQcG9wTmlDNjJZNnAwSkZaZHVkbU8xRU9oL3NlWlRiSExiZ2ZQWmQyS2RtbFNaYmNXL0w1SzN5MTJLU1BWVENDY3ZiNU41ellpVHR5Um03MWcrd0NtTEJUenpBQ3RLdHNHVStDbmNxRlFRYUZoa2hpbXJKbEJBZmtzVmtZR0pURjFpWGoxbTEvbGs3LzU3Nkc3eVZKcmh1V0ZlUnIxRUJCMmQ5c3NMTXhsMzRvNGZZUEs3TktKRWJhMjI2eXRycUFRUmxIRVlCVFI3dlVaRG9Zb29GNFBxZGZTNFYwUWVIUTZYZWJuVzRSQmtIMy9wWE9qWFBtNGEwMmxORWJTdzJVNGl1bjBCclE3WFRyZGJwcUxxQldoN3hONGl2Rm93RnlyeGZIN0QzTm8veHI3bHVkcGhGNjJ6blhaOWtVd2JKSGFtMjJVUk5IdHg3ejQ4bXZjZHpTRmgvYjZmUWFqRWFOeFJCVEZhT1V4TTFQbjZORkR6TTQwOExTQ291b29SMWU2d0R5QjFwcFJZcmh5OVFZblR0eVBHS0UvR0hCM2M1c3JWMit5MHgvRDRuNmUvcmwveHRLSlJ4bm9XaFlQcmtvK3BWaFdYVlZwQVZSRnZTcFNjUlBZbHVUS0paNXhBQzFvVDJaRm1ZeDZjOW9HQzU1VDhqYkU0VDRvUy8xWHpPdDBqc0IzaDN1NWhMbGtHRmd4WnBWblhQMysyUzNKZ3dpcVZqNXR5aUpiWFAraEU1UXdKY2pHT1V6c2NOQmNsYVNySlpJdFg5VGxoMFV4cEhOZHpiYk5xeEJzV0ZWSzBZTlZyTHZWbHp6OXhSZ0xzV0FIY0dybis4Z25xcUZLMEoxTnZ2Q3gzK1dibi9zREdyNWlmYzhxaTNNdFJBeFJGTlBwZHBtZm44dEs1Nm9KS1JYOVJGSE01dFkyQy9NTERFZnBqQ0JKRElIdnAyMUM5c0lIdmtickZBU3l2YlBMek13TXRWQVhJRTVVNVZpMTdNQkN1dk1mREVic2RudTAyejNpSkNISlNreDdkdU43bW5vdFNIOUtJeGlKQ1QyUFJ1aXhmOThheHc3dlozMTFtZmxXQTE4YjRtU0V4SEZLMnRWWjBrMldKNUN2Wm51RG1PKzk4anBQUFg2S1pqMGtTaEw2b3hHOTNwQnV0MGQzTUdBMGpoQkphRGFhckt3c3N6RGZZcVpldzFNS0pDNURaMVVxbWZKOG4zRWlYTHB5bmZ1T0hzTFBxRUNqT09IQzVSdThjZUVTU2REaThBLytQUjc1NFo5azNGd2t4aXZ0YkhhWnFWeVJsNmlLZmRZYWZxc3M4eUpIYytjNWlYWVA3YlFNVW1sNTdUN1h1blZzRG1taFRyRjFDTVdNcW95TUt6UTYrZnRZS1oxTHBiUlVjZ2h0ZzZLZ1B2N2FsaFJTU21XbnFKQ21tMWdyRDhlakw1UHJON0dPVUtVbXcwWHlxV3F4SnBUeWE5bTN0V2k3UXNoc0x1S3UvWnpoVGFHT2N0ZVMweVRkNVljc0U0UWVvZktoNWJPVTdMLzNsVkF6WTY1ODkyLzQ5SC81VGJZdW4yV3gxV1QvM2pWQ1B4MlVqVWN4dzlHUXVibVpJaVBibkJaNkFBQWdBRWxFUVZUUXdqVWltVVYyTUJpeHM5dEdLVVVqckZHcmVkUnJBVUVRb0xWS0NieE9TUW83dXgxbTZrMkN3SFBZcUNvRFFZcFdLRHpHVVV4L09LVGJIYkxUN2pDS2s1THVrMzBmQmtPY0pQaGh3S09QUDhIQkEvc0pmTTNKaHgvbDY4OStnMjk4K2EvUkdCcGhTQmlHK0RvZEp5VnhSQ1AwMmJkM2xTTUg5N0Z2ZFpHRlZnTWtJWXBHU0pKa0c0cTBMZFBhb3pkTWVPWFZNenoxK0NQVVF5OHpKNldnMmRFNFlUQk12ODkydDg5Z09HSTRIdUZwajlXbFJWYVdGcGlmYlJHR0FVcE1TalhXQ3ExRHhrYTRmT1VxaHc3dXd5Y3B2Qnp0M29qVHI1NWp1elBBMjNzL2IvKzVmOGJDc1ZNTWRTMTc1clJEWjVpb1NwMlh0bklBVkY1b2VSTkFqWkpxTkgxbEVDa1ZCdlhFaVZHYXphcTVDZFZudWRvemk3MHZzcXR5cTgxSUQ0QXpXMkxUWVNaMHlKYXBZa0tlbCs4OU0ySERKSUxwSGxpT0tXR3FKVGU5WkxZWERqL2w2aEdxTWVQMmlaNkRTTVNPUzFKcUt1UUVLNHROQ3JDQ0V3Q05hRVhvK1VnOFJ0cWJmUDF6ZjhDWFB2RzcrUEdJZy92V1dGcVlLeVRNZytFSWt5VE16all0bzVOaUhNZjBoaU9HL1JHRGJEaFdEMnMwd29CYVBTRHdQSUpBcHhESVBBYk1Rb3VKQ0RzN2Jab3pMZXBocXJ4TEN0OVJXZ2tNeHhIdFhuckREMFpqRXBPVzRzWklnZmRDR2Z3ZzVOaDl4M24waVlmWnY3N080dUlDWVZpajIrM1M3N1g1bFYvN1ZXSWpiTi9kNE55NVM1dysvVEpmK29zLzU5SWJiNlFFSWsvaktaWG1FeWp3TmN6UE5qbCs3QkRyZTVaWVdwaWxIdm9RanhHVEZDaXhuZDZJczJkZjU2bkhIc2JYVXZ5ei9IbElURUpzb0Q4WTBlc042UFQ3REFZakJvTVJjWkxRYU5aWlcxbGxaWEdXK2JsWmZOOUhLNGlTaEN2WHJuRnc3eDZRdUtnTUJZL0wxMjd6NnV1WEdPb2FKMzc0WjNuMHZUL0Z1TG1JNkpBNEdrOWNEdE5XMllVUXpmSFhXQUd1K2FDMjBnNm9hVmk3Nm0zdWdpc21iNm1xZDBVWm1OakdXOVh5TlBHYVRKbXIyVHFBajUvWkxJMTJ1cEpFVmVVSDVMSks1NkJRcU1uSlU4VTBNMVZnNWQ2NFNqbGtGaW9maG51WVZFaEYxbUxBMGcxTnJsNm0yUUJ5N2JpcWhEV0lGQ1NnaGlUY2Z2MWxQdm1mZjUyN1owOHoyd3c1dUw2SDBDc3JrMTV2Z09kcG1qTU40amhtT0k3cDlRZjBld09NR0lJd3BCYUUxRU9mSVBEeGZTOHRjYlZqWmkydzFmbkF4a2lLOUdvMDZqUWJ0YXpiVnd5am1ONWdSTHZkb2RzYk9scjRBalNhL2R5cmU5ZDUrSkZUSEQ1Mm1IMTcxMW1jbjhlWUNHT1M5S0F3aGs2N1RhL2I1dGYremE5bTh3bWRTYms5b3JIaDFvMGJuRGx6bHVlKy9SemZmdmJyM0xsK2pkRFB4RVJhb1ZSQ0hCdVVHUGF0TFhQaXZvTWNXRnRtY2I2RjU2Zm1xTTJ0WFM1ZnVjcXBreWNJdE1IRWNVRzBLZFY4R1NGWEs2TFlNQnFtQjF1M04yQTBpaGlPUnBtdGVZSFZsVVVXNStmeGc0QmJ0Mit6ZjIwRlJaTCtMblVxVTIvM1JyejA4aG0ydTJPQ2ZTZDQxei8rbDJsR2dlalMrV2wzbGVKdWlCU3VkbUNDSWFWd250di9yY3dxYTdQbHlIU2xZbm1mb3BvdDR1dUtxbjV5Z0s3dXRWR3JpSVlBMU1kZTJ4VDNCcDdpazduSGFhVXJRQTZIcEdQWCtaV3h2bFJVZTZveXJWWVZ4OTJFYWs5VmRPOEt5MHd4S1ZKQ3BvSUhyUkJSeXdZdHBSTlJtekhlc01kemYvWjUvdWR2L1FZMWlkaS9aNW1sYkxDWG1tazh1cjArUmlBUm9kdnJFSTNHS2F1dlZxTVdoSVNCai9ZVmdlZGhCenM2MmdaVnpoOVV6cU1UeFhhN1E3M1JSR3ZvalVaME9nTjIyMTJHVVZ6R2lXZU92clJmU1poYld1U0JreWM1ZGVvaDl1N2J4K0xpRXRyTFhZdFN0RXFwT0NmRmhIVjNkdGx0Ny9Ddi84Mi94dk9EOHVCVlpjeTF5VzZLZnFmTDFlczNlUFhsMS9pcnYveHJudi9XTnhrUFUxR1QxdW5Hd05QcG1yRlI5emw4ZUQ5SEQ2Nnp0cnJFYUJoejYrWTFUdHgvRU0vRTJhekZsTG1FT3NPUWtacVpkTTRRRUlqaWRKRFk2ZlhwZExwMEIwTkd3eEdlNzFOck5KaWJiWEZvM3lyMWVsQXdHL0tiOStMVjI3eHg0U29EM2VDaHYvc0xQUGpPSDJWWW0wdTNGNWxVMXUwQlpOS3JNbTBZcml6cmpsS295c1ZVWGx5bThNSTRGMzAxM2NyYW1GVmhwYzZBUjVnU1ZwalRsS1dZblVtbEo1SHFoZnY3cjZaclFHWG5pMXY3eG9LMzc0Ujc1THg4QzRQc0FCQUJVeW5ETFN1R1BhbTgxMWxaMVdGUERGaXE4Z0dIMzFlMURPU0JDV2xKblByejh3WmZ5ajVLY2dHVW9lVXJPdGN2OE9uZi9pQ3ZmL3NyekRWcTdGdGRTVUdZcE91NTBUaWlQeGd3R0F6eFBVMFlocWtQMzlONGdZL3ZlYTU4VXlianJKMUZVRmErYUM4bDRXeHNiWk1rd21BMHBqdm9rVWdGTXFZVVNaSWdDbzQvZElxVEp4L2s2T0VEcksydEVZU2hreFFreHBRdXpvTHZsMzUvZzM2ZkcxZXZNeHdPK2I5KzVaK3h2TEpLRUlTWkQxMUsvNFFxYnhDVGJUS1NPR0ZyWTR0ejV5N3cvQXN2OGR5M24rTzd6MzJMbVdhRFZyT083L3Q0dWJ2U0pNelB0V2pXQS9hc0xQRGcvWWVwKzZDU2lDUk9za3lIN0hlaHMxeUV6RTlRc0NjeUpGcWNKUFNINlRDeDF4dlE3cVh6Zy9GNFRLMVdZM1YxamRXVkJWck5Hb0d2OEx5QVhqL214WmRmWlhOM1FPUG9ZN3o5WjMrWjV2cFJodUs1c3l6TFNGTThQOVlxU2xWVXE3WkFiWEw5bWdmTldQbVlrK2JiQ3FoVlZlVDRGZHU4S3pNcy9TaVZTSFhsTURSVUtaZkpseElpcU45L2JVT1VUVW1wNnJpbktIT0tYV1MranRmYVFYZW5KNnF1M3ZXV3NVZE5jUDRueGhoVlBKZU44YkowQTlVS292amFlVGtzbEx0KzY5YlBhYXo1eXRGa1MxeHRFdXBSaitmLzhrLzV3bi8vSGVqdnNMNjJ5a0pyaHNGZ1NMZmZwOXZ2b1VVUmhBSDFETHdSaGo2K1VtalBUMEhweGJyR25qSFk0WkcyR1VKakRJekdNZTFCajI2dnozQVFaUWFvMHFlUmlKQWtDY1lZOWgwNHlHT1BQOGFKNC9leHZyNlhXcjJKS0VVVWp6RW1KZlBrVko1VVUyK3lZQkl3VWN4dXU4MnRPM2U1ZlBraXQ2NWZ4OFRwOWdFRkJ3NGY1dkVubitUeHh4L253TUg5TEN6TXBYcC9wVkY0eGJRNmYvQjFSdFZOMjRXSXUzZnZjdWExczd6d3Q4L3o5YTk4bWR2WHIxSVBRMExmVDFkOUNIRWNFeWNSNjN0V09IWmduYlhsZVpibVoyaUVQa2tjZ1VwTGVVL3I4bDdVRmhPQkxOUkQwaFprSENYMEJ5TTZuUjY3M1I3ZGZsb2RDTUxpNGdMNzkrMWxlV21Kc0Y3bnpObnpuRGwzaVZGOWppZmYvNHNjZi9vOURGU2RSSG1XaDZCU3RjcGtVVHdkZE91bURsV3pMNlRxYzNHOExhcXNSaVpvdi9uRlp5bHJIRjJjdUhKaXBTYU1lN2JHd0prQjNLdHJtWmlNQ2xYQS9mZkREdjRmL1NXVjJPZnAxWUJ5SkpNbHFVOU5ZTUZseXZjb2xweFpsQ0x3TktIRWRLNWY1RE8vL1p1Y2UrN3JoTDZtV2ErbHUvSGhNSHRSRTJaYkxWck5lcXE2ODhxTnBKNnlNY25iRTZOc21hWUd2QlNuM1U5dnIzYTNTMnpTQUZESk10NlN4S1NDS1dOb3pEUjUvS21uZVBEQjQremZ2ODdjM0FLZTl0SmJYbFV3NjluRVgydU45anlVS0RyZE5qZHUzdUQ4dVF0Y3ZuQ0owV2hJdmRta1VhOFRCQjVLd05NSzMvY3hDUEU0b3RmcmtZaHc5TDc3ZVBycHQvSFFxVk1jT25xVW1kWXNTaVdZekNLY2wzdXF5UGNyUDU5K2Y4RFY2OWQ1N2RXemZPTXJYK0hacjN3Rm9pZzlwQUtQSUorRmlCQUVpa1ByZXpoeDdEQjdWeGVabTYzakFYRTBUbkZxWWpLZkFZaEtzcGJHTHhwbXlYb2JJNG80Rm5xOUFiM0JnSFp2eVBiMkRwNm5hY3pNc0x5MFFxOC81TXExNjR6eFdYenc3L0RPbi91bjZPV0RqRlJRa0p2U1BidFVBVDFUK2FKT3VlNXNtdFRVZDBTWTVxMnBBRHNyRzhQcWY2K3FiY2s5TVBzbFhsOHFhZDZTSGdCdkFrMmRjaHBVM1JGVHZuUDVQM2pycDdvQXNhb0FOVUZ3VVpPend1LzdBNGdqTXNuVmlJWVpNK0xzdDcvSy8vai8vaDNTM1NId05hMUduVVl0SGR6NVhzRG0xaGF0MlNienN6TUZXY2ZHbzVmNjdXekZranVjQ3hhY1pqQWNaYXkvWHJyL3BveW15dXNGWXd5MVdwMFRENXpnc1NmZndwR2pSNWlmYitGNUhrbGlzajE5ZXFQbkdoT3QzQVRjNFdERXh1WUcxNi9mNE9MNUMzVGF1d1JCbU43QStZcFIwcHM0U1dJOHBRakRBTjlMK2YvYTg0bzBZaFNZeEdDU0dLVjlUcDU2bUVjZmU0UVREeDVuMy9vK2F2VjZWdUs2Q1RuRkprVUVyVlBSMHM3V0R1ZlBYZUE3TDd6RWwvN2lpMXc2K3hvaGFiSndFQVQ0bmtmZ3BackxtVWJBa2NNSE9YcGduWlhGR2NMQXh5UVJTVFJDSkM1LzF6YXRKM3Y3ZE1hZ1ZOb0g1VEVZanJsMDVUb3pzL09FWWNERzNRM2FuUjZkZm85QkRPUDZBdS8raFY5aC9mRzMwL05DREJvdGsweG5aUkZIN2JHQjFUVzR0dWhwajJhMTU2L2tZU0l1OGJKYTZrOTFDdHExdENxdHhrV25NRzM2amNxR2dKWHdpcXFVdEdxYkxkc2hjYXl5RXo5UXhWdGh0dyt1L0hheUFwaEdmclZuTGxWT2daNGk1VlNPM3Q5S25za216elZmbzAzQ2FPc1dYL3o0Ui9qbTUvNlF1Z2ZMaTNNc3pzOFJlT2xEbEppRXUzYzNXVnlZcDlFSU0rR1JzU01FSGZoamprWVRZQndaK3YwQnU5MXVpdWsyYVlxdkVVVmlESjd5TUJpTUpPdy9kSVNISDMyRWt3OCt3UHErTlZxek15U0pMdERrMnZNd2NVS1N4R1ZKTDRMMkZGRVVzYnV6dzQzck4zbjk5VGU0Y2YwbW50YUV0VHFCNTZFOWphZlRGM2t3SEdJUTV1Ym5XTjJ6eHVyS0NvMndoa2tpSG52c0ZIYzM3bkQ5MmswdW5yL001dFlXZ1Jld01EK0xLTUVrZ2xGQ05CelQ2L2VZbVdueDFuZThnOGVlZUp6Nzd6L0c0dElTWVJobUpOM3NjOWE2OUdKSm1iSVVSU051WGIvQitYTVgrZGEzbnVQWnJ6M0xqUXZuYU5RYkJKNkg3MnM4blUrZkV0YldWcmp2NkNIVzE1WllubS9pYTQxSnhwQ2tya1B0WlZKMlRKWm5rQjhRT2F6RTUrTGw2d1Mxa01PSDlwTkVFWVB4bUV2WGJuTDIzQ1g2U2NDUkgvZ3gzdjVUL3hDWjIwT0VYK1Q2MmFsRjdyQ3UwbnFpSnNwVnBlNVIyVlpmU0dzSWIyY0x1ZzVFS3lCRXJLU2ZhWDZZaWRYQ3BBMUJmZXpNcHRpOE1LcnJNK1V5cEZKdHZDNEhmbExkeXl0cmFDSTJHOVVhWHRpUVpTb0dUTmVZcE5RRTlOamFvZHFrV0NsdklVbzBjeFhHYkxLNE5ROUQzWXk1K09KemZQeURIMkJ3NndwTGN5MzJyUzNqNlRMMGRCd2wzTjdZWUcxNWlkRFhFNjFKT2MzWENKb2tTZWlQeHZUNmZicjlQbEdjRkhwdTJ5cytqbU1XVjFZNThlQUpIamp4QVB2MzcyTjJ0b1h2ZTluaG9yTlRMYVZQaUU3bHJDb0x0aHhITWQxT2w5dTM3bkRwOGtVdVhid0lScWlGTlh6UHo0TEhESklZaUdOa1BFYjZmV0tUY09DUlV4eTUveGphODdONWlzSWtFVVFqL3M1Ym55U09SMm13cXZJWkRrZjgrZi84UFAyYk4vRmJzK2laR1ZUZ2syUUhrcGNsOEtiMjRDRjc5dTNqMFNlZTRLR0hUM0hreUdIbUYrYlJ2dStnM1BLQmF6Ni9TUU5MWU5BYmN2UG1IVjU5NVF6ZmVQWlpYbmp1bTNTM05xazM2dmhleWpmMGZCaVBSL2dlM0hma0VNY09yYk4vYlpsV1BZMGhTNklvODJtUXpTd0tMV3VxcHRRK1Y2L2ZaaHhGM0hma0lCQWpRRzhRODlMTDU3aTFzWTFlT2NTNy90Ry9aUFdCeCtnVElIaGx6eThWczdxeXNabVQ5M0VCeGJIU2pLdVMrMnA3bS84anJaV0xyUmZiMENNRk10MG9WL0duS3FWL0ZYU2pMTkdlK3VTWnJXdzVWTEh4V2xGRVVvUnJZVTNLeGVwRDNJVzdxUXovUkhKVGhEaFNZbVAxSTBybFJoWDM3UExzTERWRk1iQlRSanROdmtVR2N6aUF4a2hoNzhwL1BsOWlhRy93VjUvNUJGLzZ4Ty9SME1LaDlUM3BMU2VtYURZR296RmIyN3Vzcml5bTZDMXJ5eUZaQ2syY0NNTlJSTHMvb05QcDB1c1BNS1M5ZERyUlZvVkp4ZzlxbkR4MWlsT25Ubkw0eUJHV1Y1YnhmWjNKYmswNXpCR3JWWkZzeUtxRVhxL1B4cDBOTGw2OHlJV0xGK25zZHFtSFFTcUswV21WNEdYR25XUTBadEJwRTdmYmVJTUJNMFpvS2lHZWFiTCtRei9FL21OSFNaTFVjV2ZFa0l4SEVJOTU2OXVlelBieVFsZ0xVV2orK2xPZm9YYm1OWklvWml0TzJGR0txRFdMdjdCQWMzWU92MUhIb0lpU21DUXhhVnNSUnlSSnpQM0hUL0RJNDQ5ejh1UkpEaHc2eU54Y0t3MXF5ZTNOYWY2UW80Zk53MXAzMjIwdVg3ekk5MTU4aVc4OCt3Mis4Wlcvd3ZNOEFpLzkzZnJaeWxHU2lKV1ZlVTRjUGN5UmcrdnNXVm1rVWZNd1NVdzhHaGVXTUtVVW5xY1JwYm16c2NQdDIzYzRjZndJZ1dkUXlnTVZjT242SGI3M3loc01WTWdENy9sSm52alJuMkhVU0tYRXJ0N0drZ3ZiZ2hjbVZXclZseThIRFphK2ZTbms5dmFscWpJdlRQN09sUmthNWI5ckpsZ0VVNUkyWllySUtGODVmK0tzclFQSXdBUlNZYUdva3Jsbk10elJ2YW9ObWNJcHNTZW5FNGxGTnRTck12R1RLZisrVXgxWUZZT1dFdVJqLzJVeXNvL24reUFHUHhseis4eExmUHcvZllBN3I3L0MwbXlEUSt0N0NUeWQ2K3RBS2ZyOUVZUEJnS1dseFFJeGxkNyttdEU0b3BmWlg5dTlIbkZXVnFRRHUvUmxqK01ZQXh3N2ZvSW5uM2ljSTBjUHM3NitUclBSU0k5VVl3cWFjRzZnU1FHaWFkK3RQWS9oWU1EZDIzZTVmUGtxRnk2ZVQxbi9uazlRcStINVhyNGdRK0tZNFhCQTFPOWp1aDNxd3dnVmpRa2xvWWtRSklJMnFhVzNOOXRnejN2ZXc3NzdqaU54T29OSWpHRTg3RU1TOGZUVFQ2S1M5T2YxdkFEUDgvbnFwejVONi9STEJGRkVMSkJvemNBUHVURWNVV3MyNlhzZW8rWU1lbllXdjlFZ2JOUlJHU1pNQktMeGlHNjNoeCtFM0hmaWZwNTZ5MXQ0NE9TRDdEdHdnR2F6bFQwNFpzSkJLZ1dvUm9qR1krN2UzZUR5NVN1ODhNS0wvUG1mL1RtdnYvSUtvZWN4TzlNazlEMTgzOFBYUXVEQjZ2SWk5eDA5eU1IMU5aWVdaZ2s5VFRUc2s1aW9jRnJ1ZG9hY3UzQ1JVeWZ2WjZZZVpqa0dQdDFCekFzdnZjcnR6VjNDQXcvd3JuLzRMNWc3Y3BKeEppVzJWWnJLcVk0cjdJQkszMjFyL0tlTnJDYWVYM0VWZlVaVjhyZlZaT3J4dEJYajVQZFIwb3pVSjNJZ1NPSHpTaWJCeXNWT1ZHTWt5VTV0Sy80cVJ3OUxxYXhXT1pkTnNJWmgwOXFMQ3F4VDdqRjl6TDYrVVpUUno1VVVuNm80d242SU5BbjBkL2phNS82UUwvNytoL0dqSVlmMzcyVmxJZVB6WmFocWxLTGRIWUFZNXVmblFDQk9FZ2FqRWUxdWoyNTN3RGhPQ2g1Z3ppVXl4cEFZWVc1K25pZmY4aFFQUEhpY2ZmdlhtWjliUUNzaE1TbVIxOHU5L1hsUVNCYi9wQldNeDJQdWJteHk0L3BOenAwN3o4M3JOL0E4VFMyc1pSNzh0TEx3VUVnMEl1b1BTTnBka200YlAwNm9HVVBkSklSS0UyTkFEQTNQUXlXbU9KeDdzM1ZXMy8xRDdEOStuTVFZbEVtcmdORndnSW9qbm43NnFiVG1VcWtmUVd2TjEvN3dqNWo1N25jSnNnd0FyWVNSOXJuYkg3STgwMG9QRmxGME1IU1VabFNyb2VibUNXWm44Um9OUkd2aVRKTWdrcjdNbytHSXhzd3NiM25yMjNqNHNVYzRkdXdvYTN0VC9RTG96RXNnSlRRUlE1S2o1eFZFNDVnN3QrN3cycXRuK2ZhM251UExYL3BMdWpzYmhMNVBzeGJpK3hrTk4wa0lBNTlEQi9aeDlOQitWcGZuYURWRHRJa3hTY0k0TXB4NS9UekhzM2p6dkk4MzRuSHh5blcrYy9vc0kzK1drei82c3p6MnpFOFMxZWNSclIxeW1LNG1VazNabzZtSy90OGRZRTh4OGhma1hsdWRXSjB4dUFlSEtkeXIxVXZmRFQxVlJZU1pvUDdnN0tiWXZ1VnFIeTk1V0pjcWUzWmJHSVRJSk5tbjBzKy82YjVRS3JBQ3BxdWM3SFJVVGRWUVZCQUIzSU5GUVMzMDhPSVJ0ODU4ajA5KzZOZTUrdktMek5ZQ0RoL1lTejNRcGRBcCsxQjNPMTM4SUNUd1BmcURFVHZ0ZHVyU0U1QXMxOUJrQTdqRUdPck5HUjQ0K1JDUFBIcUtnd2NQc0xxeWpPLzVKQ1pPKzJpdE1wV2V6akJkMmE1WEs1SWtZbnQ3bDFzM2IzTCtqUXU4Y2U0Y0F0UXp6TGZLRW0rMVFKS01pWVlqb200UDNlbFFINCtZRVVWZEpLVVVaTTVOTDV1TERMTXBTTU5MMGRsNTNIeTdWV2ZsM2UvbXdMR2poWHZQbUlUUmFJZ2FqM242N1UrUk4zR2VINktCWnoveldacmZmWWw2bkpBdEJSaUp4OTFlbjVWbWt5Q2pKSWpPbkp5ZXp4REZyb0ZOZ1g2dGhzek9VWnVab1RiVFJBZCtaa0hXbU1Rd0hBNFlqeU1XbDVaNTY5dC9nRk9QUHNiaEk0ZVlYMWhBK1RhaVRxeGhYRFppbEpUaDJPc051SFRwSXQ5OTZYdDg5Y3RmNWFXLy9Sdkd3eTVoRUtDVlJ5M3dzMmJTVUsvN0hEOTZpQ01IOTdKdlpZRjZ2YzY1ODFkWm1HK3laM2tlaWtzT2Ruc2ovdmFsMTlqcURnbjNuK1NaZi9JdldUcjZFTEdxTVloSFpUc3V0ckxVYmxrcjNnS3ArSDd5L2IyZGJHd2IyaW95LzJvNDZwUWRPbFZUVDhubVZFNVZBYUErODBiYUFtZ0hsbDI2QU1sRkhwWUNVV1JTSUpUZnRGV1JrQlc0bmpyV01qU1VyVllTUzdTVEN6MmtPTEdrNnA3TTRJeGw4R2NlU2lqaVdvVTloTnE0eC9OZitoUCs4RC8vT240MFpPOXFtbStuaTRHT2tPQVJ4NGJ0M1IzNmd6RlJsTjRPMnROWmI2WXk5MXFDVWg1SGp0M0hRNmRPY2V5K1k2eXNMTkZvMW9uaXFHTHRsSEkvYmRJQmxGSXdHby9adUx2QjVVdVhPZi9HZVRxOUhyVjZuY0FQZ0hUam9JMkFNVVNqQVhHbkM5MHU0VGpDRTBGRkViTW9mREY0U0FwMVVOYWhuTVZxajhSZ2xORHdVdXlKbC9YVjdaa1psbjd3SGV3L2VoOGlDVXJyVkFBa2htUTA1UDAvOWZmWTJMaEZ2OWZIU0Nvbyt0Yi8vRHlONzc1SUkwbUtmY3RZUE81Mit5elBOQWd4S0NWb25TY3NwYUlsWXhRanBlaDdBVmU3UFJyTkpwSHZ3ZHdjL3NJYy9td0xGZFJBNjR4TXBFamloR2cwWWh4SDNQL0FTVTQrY29vVEQ1emd3SUg5ek13MnMzbEkvbXg1RnVJcWc0bUlBanp1M0xuTGhmUG5lZUg1RjNqMmExL2p0UmUvUTcyZUh1eGFhenhQRVhpS1FBc3J5NHNjUFh3RU14Nnl0dEppLzk1Vk5Fa2FhUWVJK0Z5NGRvZXpiMXhpNE5kNDdIMi94TWwzL0FoUmZaWkl5aEd6SSs4dUFudlN6eitQSkM4SlMxU1NwcG00Z08zRUxsdHA2TXFIeS9mTGJrSEtqWU05a0xSRlFOblgvTnk1TFZFNVR0anh3YVcwSEpQWldWTVJUSm02bWl2eThqOGdKd2FuaUhETFJHRTdHeXhHZW83am0xUlNWUkpVN0kyb1V2Y1FLSlhIYXZyL0docktzSDM1RFQ3N3UvK1ZjOS8rS3EyNno1SDk2OVREa01na0RJY2orb01oN1Y2WFRuZEFIR2RyTlpYbTVlVkR0U2lPMmJQL0FJODgrZ2dQbmpqTzN2VzlMQzRzWkhBZFU2d3NwVkRwcFpMWGZMWGE3dy9ZMk56aTJyWHJYTDU0aWR1Mzd4RFdhc3pVR25pZUIxNUdWMG9Tb3VHSVlhZURkSHY0dlFGaEV0TkFDREQ0UUpTazMyTmRlY1UrT2o5MGN3bXRDQmpSREJLRDBVSkRLM3dsZU5ubjJtbTJXSDdtR2U1NytCU3RWb3RXbzQ3bitYUjJkdG5kM3VUWC9wOWZRMmtZamtkMGRqdHMzNzNEbi96VzcxRC9tK2NJeGtPTThraVVacGhvN3ZiVEE2Q0c0R2xqOFFyVE14bVRFbTJHQ3U1MCsreGJtQ1AwWUt3MFBReHROSU93aHN6T28yZGJlRE5ObEI5Z2dOZ2t4RkZDRkVlTUJrT0NJT0NCVXc5eDZ1R0hlZWloayt6WnY0OVdhejVyQ1hUSjdjMjNOQ1l1QmsvRFFUOXRxODY4enJlKzlXMmUvZHJYdVhiNVVqbzc4SFJhcFVHYVRLU0UvZXRySER0OGdMMnJTeXpPTmxOdW94L1NHMFI4NTZYVDNObnQwenIyR0QvNHM3OUU0OEI5REtrNVhENHA2dmFDK09tVzlFN2w3UG9HeXRhMXpDaVdpZy9tWHNNM3VZZitSZUZLakF1a3VJRDZzNHU3S1R0QTJlUlNLZm9UayttK1RYWTdtM3k5SkhrbGtJY1FLSXRIWG1iOWl1TVhuSEFaTVpuSFVvM1AxcW5DeTJMNTNVT0lpT2NweEVUNGcvK2ZzRGVMc1N6THJzUFczdWZlTjhZOFprYk9RMlhXWE5WZDFWME5pa00zUmN1MEtNb2lBWEV3TFJpV2JNaXdiSC80d3dQZ0x4STI0QS83eHdac2dOQ0hUTW1DUVZtVWFabzBtNlI2S25ZMzJYUFgwTlZWbFZXVlEyUm1SR1JrWkVTOGlIakR2V2Y3NDB6NzNCZEpmM1JYVldiRUcrNjk1NXk5MTE3REFOLzkxMytFMy92dC94bDBmSWlMRyt0WVdWcHc4L2lEUSt3ZkRuQXlHcUt5THR3ak1hVXNKclpHcTkzQlp6N3pPcTQ5Y3hVWExsekE4c29TeXFMbHF5RkpDU3dxR05TRmdEQkc0eEgybmp6Qi9idWIrT2pXeDdpM3VZbTZGclJhTFFmY3Nic0pMV0hZZW9KcWVJSjYvd0RtNkFqdHlSaHRDSXJhd2xqMnRtVUM0eXVha1RnT1Fkc1lpUFdXVTM2VForOHdGQ2FzSjNVTks0UTJDVm9BZXFWQnUxWGdxRCtEOWIvMWkzais5VStqcW1vdmFpSU05dlp3c1A4RS85Ri84by9TREo4WnFDYjRKNy81bTdqMjR4L0FuSnhnNzJTRTNZTmpiQjlYZUhBOHhKbTVHWFJaWExaQ0VLQklDa2NsQ0Nac3NITXd3Tm41UGxyUm4wbGdpVkd4d1pnSlJ5QU0yR0RZN1dMYzZ3UGRIcWhWZ2dyalJxSCt5S3pxQ3VQaklYcnpjM2orcFZmeDZkZGV4NFZMbDNEbTdCbTAybDMzeW1JQnN0R01SVUtWV0R1K3hlQmdnTnUzNytIdHQ5N0ZONy94bDNqelMvOGFMQmFkVnVsQ1U3MmxuWUZndHQvR3BYTWJ1SFQrTEZaV2xyQzB1SVJiSDkvQkQ5OTZGOE95ajlmKzdiK0hLMi84TEtyMnJCc0QrK3FXYUhyV1R4a2VKamxOWHloV3ZIS2F4MFptN29HTVhoeGRzT0pvbFRNVVVVaEQ4cEtMbmI5MDkxQ1NaVlVTTmRqWVp3RzFwMWZXMWhGZ0tnbWJRbEE0V1ZnTDFFb1lGTDlNVk8xeG5NZW1ZRUJmMEl0MTNJSlRBNTJuWklPbmlESzlJbzVxMUxzUDhNLyt4LzhlNzc3NUoyZ1hCbWZXMW1DckNvZUhCNmg5VDI1RHFTdzFKbFVGTWdWdTNud1dyNzc2TWk1ZHZZelYxWFcwU2dPUSsxNGlOZXJnRDZjU1hOaGpFZ2Y3aDNqNFlBdTNibjJFVHo3NUJFY25KeWlMbHB0ZGV4S0tNN2VaWUhJeVJEVVl3QndQMEswcWxMVkZZUzFteUJtd3hJY0VLWHMrNUJXTXJVVXQxanNHaDhoeG4xTkhoTkx6OHNHRWtUanZ4UFdaUG5wTTZCUU1ac0ZlMmNiY3ovOENibjc2VlVkeThhZkw0ZDRlOXA4OHhqLzhULzlSQ3Bjd0ROUVYvdWx2L1JadXZQY2Q5Q1pqQUlUS0VoN1pBdDk0N3hhZXVYQUdORHJHNE9RRVk4dW9MV1dmcXlCQ3hZeEhod09zei9aUlVzZzU5QTdIbEI1NkljS1FDM3h5Y0F6cWRuRUNZTlJwZzJibjBacWJRYXZUQlFYNU1UdGZnc21rd25nOFFuOXVBYTk4K2pXODlNb3J1SHoxR2hZV0Y4REdWWU9oelp5MnRpWlVsY1grM2o0Ky9PQURmTy83UDhDZi9QRWY0KzZIN3p0YXRERW9tTkZ0bDI0enFTdjArbDFjdm53SnBTbHcvOTQ5RE1WZy9yblA0Z3UvOFEvQnkrZDhsWnliOEVDTjd6QTFDWENmallXeUFBQkxvcVpvT1dnVzFwT212MnM4emZvMldhZFpSZitDa0dYZ040V2lYU1R4QTBuSVNmY29vZlhsSE56aXIwbGdhNHBwcTVWMWJVSmRTK3hwTEttQWpSZ1d3U3JZdzAwSTRBRzEyZ1pLWjUyQ25vV1ZGbHBTU3dFZGgwenFlN3RQUE5zeStKMy83Ui9qN1RmL0RLWC9tKzJkYlJBY3VhYXVMWVJxa0Ntd3RuNEduM3ZqRFR4ejh4cFcxOWJSNjNTY2JiVTRqdm1rQ3VRUmQwMUs0OEFrc1RXT2pvK3g5WEFiZDI3ZnhvL2ZmeC9IeHlkb3Q5b295eFk2N1M0Nm5SNVFXOVNqRWNiSHh4Z2ZIOEVlSHFBMUdxTXZGaTJ4YUl1Z0VIY2RhN0V3eG8zMVBPOG5WazlpYzljWDhadU95OFJ6SWg4WE4xNWd2dDFDcnl6UUtsdDRQQnJqWURoRXIyQVk4YTJKQUJWYlpZeUtYR2ppclpoQ0tjcis0TFR3WkoyNmpxZDhoeGtkVStQUzBod1d5amxNYkkzRDR6RWVEMDd3YUgrQTQ5SFk2eEVNV0JpMkJpcVFjNW55VXlOVzZVenM1K09tbXFBekh2M2pIQzRBQUNBQVNVUkJWR0s1M1VLTENLUFJNWTZISnpoNkJCekNvSjdwbzdXNGdON0NMRnJkUGxwRkI3MWVHd0R3OW5mL0F0OSs4OHNZMXhhTHEyZncybWMvZzJlZnZZRkxseTloWm00V3hwUTVZWWVBb21Xd3ZMNkk1YlUzOE5tZitCeisvbi93RC9EZy9qMzg2TjEzOGVkZi9UcisvR3R2WW50M0M3MU9HNzFPRzhjVHdUcy92Z1ZZNno0ek1mYS84elc4Yy9VNWZPcHYvUnBHY1lGS0RzSXBQb0JWV0lGVDFWckZkYUVHOFMydnJtTlZIUU5KZFVwMXc4NmNVbnFSS0FLUU13QjJvcTZpWlNoeTFia1J4Q2xlZlZXTEswT3RCU29BaFQ4Rm1NVE53QTNGaEZheXViZWZ6Zno0SmVxOHcwN0duQ1MraGdPdlBZbDBVdHlZTXIvMFZ0bkJUSUw5QlJwWHdFdXZmdzVmL1ZlLzZ6VHB4RjRoTjhIS21UTjQvdmtYOE9LTHoySGozRmtzTEN5NEhuOHk5cU8rQ2xicTNMalRQL2pqMFJnN083dTRkKzhoYnQvNUdOdmIyeWlMTnZxOUxscEZHNjM1dHJ1WmRZM0p5VEhHUjBlUXdURjRjSVJlVmFNbGdzTFdLUHgxWkJVeU9nRmNVQ1JVOUJtRjlvbGd2WjIzRFZ1a3VERm92eUIwaWhLdHd2di9FOE9JZ0dIQlVzRklqWklJaFFMSUpIbzFCSURWVTVhMWpCdEpMMndEbUJyOC9vTGsxR01rSlRrcU5VOXFGSGFNZVdiTXpMUndjVzRGRXhBbVZuQTRtdUQrazJPSEtOVTE2a0R3b3NUbnNFRTlpUlFuVmhMUWtob2RJY3pDb2hLM09jcndHQ2RiSnhoc2JlSEFsTEJ6czJqUHphRTk2MHhTZWFhUGpoV01qZzd4dFQvN0UzenhEMzRmdGJXNDlzeE52UEtwVCtIbXM5ZHg5dHdHZXYwKzJCU1J3ZW53QzBhN1UrRFNsY3U0ZVBreWZ2NXYvaUlPRHc3d3lTZWY0SjIzM3NaWHYvd1Z2UDI5YjZNZWpWQVVqS0l3b0lKUVdjYWxtOCs1KzBPbmxObXhJbkt0czh0elRHUXZJdmZkSURZNzE2RTJFK1lFK3BIM1VBQ1RhaXNFUnRPT1ZEb1gweWtHUGQ0NHhabFZjL3E0bkNYNE9NVFYrSWZIc2tVcERxQmhJckFsTUJnc1FHV3RlMGdpTGRhVno0RWZIK1hHcEJKYmhmM2NVbUtMVHdyb0N4NXMwWHhSS3dGRHpqR3pGNk81c3ZmRm4vNDUvTmYvMDIvalQvL1Z2OEJiZi9ubldPNjM4V3UvK25keDQ4WjF0Tm9kRnpYbHhUUWdRbG0wWVAwSnlUQ0FPQXZyM2QwOTNOOThnRnNmZllTN2QrNkIyTGcrbmhuZFRoK0ZNWkJKRFRzK2hoMk5VQTBHNEtORHRLc0pab1JnSnhPVUFwUldNdkdVbHBKYXdNKzJmYnVscko1REc4WE85dFV2UWxlZHJjM09vR2NJTUlRSk00WUNTQzNvQW1oNUVNcUtnUDJteXBaOHl5WlI0eDluVjFDUlZXRmNGTm1haVEvaUtnaEpFWHhpblV1d1dLQ3VZU3RnVUJUWWt4cUdHSE90QWgwQyt2MHVlak45UEJrTWNHRjVIaklaNFdnNHdyaXEzT0luaHZYT1IrUTNIUnMyUG5iUEZnTm9HUUtMZ0tveFpoaFlZc2JPOFFERHdUNjYrMzBNck1WaDJjWjRaZ1k4TTRPaTJ3T1ZCcjF1RjlZS0h0eTdnM3NmZjRSL01UeEcwV3JqaFpkZXhxdWZmZzNYYnR6QW1ZME50TnV0NkR4TVZFUUVmVzVoQVMrLytncGVmdlVWL01xdi95cDJ0aDdnOS8vbHY4VC84L3QvZ0tvOWd6TlhiK0J6UC9jTFdMMzJISWJpMktiNjlDWlZVYVhEcjBIU2FUcFNCODhIa1ZpNmMrem5mYVJMVXZuNFF5RklzNVBaYnBqaXhLUWluU2JzWDdGdytteE8xa3pCVEZEY3pTQ3ZMUmYvWXRaL0IwTVdKb3p2ck85eklhaUNGWlZRU2xpSVl6NlZOb0trRlloSWFaeFpzakpjQ0R1ZWlqV1NuQW9WS1pRV0dGS0JNeSs5anIvLy9Nc1k3MjdoZi8ydC93b3pNejJBZ1BGbzZQWHh6Z3dqN0xCVlpYRndzSThIbXcveDhVY2Y0YU5QUHNaNFBJRnBPY1pkZjNiT3NSU3RvOHlPVGs0d1BENUJlWEtDYmozR2pEQTZJbUJiK1J0QkdOcktCWGx5R1BlbzNEWmxwUlRNTmF3UDZvQTQ4OUdDQ1oyeVFLY3NRY3lvdU1DajRRaFB4aU1jRmdhUFNYQlNHSFF2WEVKbmZoSDcyenVRMjdkeEVZeVdXSWZiWkFhS1dqdVJRTG9JMmxLaXRjYXdGS1hERUMrMENadXlHMUc1RVpkbHdxT2E4WjJoeGRINk9wNzU5R2Z3eWYzN0dIM3lIaFlERHRDZlFiZlh4bkxaQm50UGdOR294dDd4RVhZSFF4eVBKNmpJQUZ5cXNWcXV0MWZuRkFvQWJSR1VVbUdwSG1IRkF1TjZnb1BqSXh4c2IyTkFoRkczQzVxZlE5bWZRZGxwbzJpWDZIYVhZQXpqL3UzYnVIM3JRNHhHWTh3dExlSDVsMTdHQ3krOWpFdVhyMkJ4Y1FGbFdUcmNpdW8wUml1QXRYTXJ1UG5jTS9qdzd1djR3ci83SDJQUzZXTkNCY1prRkhBZHZ3RnNOSmIzOGJuYVlTeEwxTFp4ekJmY3JZUW9wK2VIU2tMdEdDTEtneWhPRzhSWGtlSFk1OXd5VDVKelVCRldEb045Lys3N2JQYUFsRW1KdXVFRGxtSlFXd0lacDBVM2xrQzFBL1BJMXBoSUlnM1VjRHQzZUcxU2lTdmFIQ0diZFlwMkxwTXNuQ1NLR0NpTVhDaGpYZ0lDVTdSaGl6WmFpeFpVdE1HS0h4WGU5M0IvSHc4ZmJ1SFdyVnU0Yy9zT1RvNGNBYWdvUzhjMWJ3SEdBREllb3pvWm9Ub2NZREk0UkdzOHdSeVJPMjBoWUxFb0FqY3l6TUhWN05adEFDNkFJOU5ueDNSaDkwME5HTDJTMFdtVjZIZzNJVEJqd293VEFQdGk4VkJLRkpjdll1YUY1M0hoNmhXc256MkxtWVU1UExqL0VMZmUvUkcyaVZEZi9qaFdBRllGbVRKWmxlSXd6YjRLbnZHSm5lMU1TbUtWSUFsZGRsd0RHNmRDQXd2ODRNRU9xbGMrZzk2Wk5meWJ2L0gzc0xDNGhLT0RQZHkvZlJzZnZQc3VSbC8vSnI1Lzd5T2NheG1zRklRWldIUmJGbWM3QmRibVp6R3VCVWZqQ2p0SFkydzlyc0cyY21XdkwyclplcXF3MXhBaysyd0xXQUZiUWNzS0Zva3c2LysyT3E0eE9EN0NBUWc3MXFJOVA0L3UwZ0xNN0N6S2JnZEZpMkdvUkgxMGlCOTg0MDM4eFZlL2hMb1dMSyt0Ty8zQzh5L2k0dVVMV0ZpY0F3cmxITTBFNnZRZy9VVlVwdTAyUVZzN29OQW1BbENVd2ttSWw5TlpGVW9aNlAwZlV2cnZ0S3RWUHU3TGFHOEtNN0JwNkNDU1NlMGtaQmtJc2hhbENHaGc3Ukg2Nk0yVzJZTnhGb3hIM3UvTmdVWnVkaW9HSU92Sy8vQndjQXdHVlI1K3A3SjlxYUYxRnZXd3B2R0doY3B0bDlUb2FFRUVFYU8yM2hlTm5KQ0dER0Y0Y29MTnpRZTRjK2NPUHZqZ1E1d2NuNkRWYXFQZEtoM1MyKzFpT0hRa29QNU1INzJ5UUgxeWpNR3RqekJiVldqYkZLbmNZOERFZmtiMWV4S3drS0JPRFA1N0taaENQQWhrQUhUS2xzTWg2aHJMQzNQT0tjY1k3REhqQklRalltQnBHWmRmZmhrdlh6cUhMNnl0b0RjN0N6WUd0ZCtVSitKd0dPdEpKOFpQSjRKdGxUN2RYZlhKVXg0TXFUcVYzT1BPUVkweDhZbGsyZ2VDbVRDcW5QeVd5VzhNSUpqQ1lINXhFYk1MQzNqbWxSZng4Ny95eTNqeWVBLzNQcm1MMisrL2orLzg1VGNnRHpjeEQyQ2xaVEFqTldaYU5WcTlMdTd1UE1JTEY5Y3hHWi9nOEdpSTBmRVFGU3lZQzBmMFlZb2hHdGJyM21PVkpVRHBrZlkyYXZRRW1HZENlVExFYXFjRjJqckd3UU9MN2JLTjR1eFpWNDJSd2Nsb2hMSXMwZTMxVVIwUDhQYTMveExmZnZQTE9KNk1zWDcyUEQ3MTJkZHc0OFlObkR0MzNqMW5YbkZaMTNWUzdOa0FvbnNrUHVobjRxbXNqV3lhb1RSV0tRUXBmcGRtdHBNR3dxa1JJVUxLQ1lnYWhxTWhTcHpKdFlQaElDMG9vb3c1R1NFRUJRU1hsVWdwSmlRVFI0anp2ZU5hT1Jhekx5OGR1aDIrbEtGYzdoUTFBekdHV3dXc29lSDU1eVduUkJaTTVxbUdJeUhnUkZSQ3NPVUMvL3MvLzEyY25CeWoyMnBqWnFhUG9pd3h2ekFQQUJnY0hxRFZLbkh4OGtXc3JXMWdkbjRXaC9zSDJINndpVmxtZlBMK0I1aURFMENkV0VFTjYwOUNTWXNuWENMMm5uYmVnanVNL3dyL2djdkNPK0NVTFdmdVFZeXFxbkZRMTloa2cyTW0wUElTTGovL1BLNWZ1NDZ6R3h1WW1WOEFNVEN4RStmckI2RHlpOStLQldwQmJhdDQvNWdaUlNqVC9TWm9QWEFZZ0ZxSjUwVWQwNWNDQUJmT0ZpSDJZMDZLVVZneDY0RmMva0RvQmRpRGRoTUVMS2lDU0JXZkZjTU1NaTJzbk5uQTZwa0wrTlFibjBQMTY3K09SdyszOE1tSFA4YXR0OTdCKzIrL0JmUG9BV1lFT09wMllXYTZXQzU2T0U4RXFXc01oMk1jbmd4eGNEakEwY2tJRlJsWUVmK2VBcXB0SWdSUjdyTGNBV0dXTE9idEJHMFNMRE5EUmtPY2YrRjVQTnpidzA5KzRXY0FOdGpaMnNIZE8zZng4VWNmb1YyTk1EdlRSOWUyTVRyY3c1Zi84SS93Zi8wZnZ3dVlFak96YytpY3ZlRWk2anlKS0V3MkNDb09UNVhkUWFnZWdkWHNISlk0dUNPa1FCckt3ai9UaGh2L3pMdTFra3c3YVFzYTZjUkUydFU4Wm1RV0RvWFZ4RjJkWG1LVG13ank4RTlCN1UwYnlTVzJTSTJDdllwTTNLNFhTQWd1a2dweDdFRWVPSXliQVp0RURjNVVnNVN4bVVDY01aNlNydG1Wc093M3NqZzdMYnI0OS83ei93YTMzL3NCZnZpdGIrS2pkOTdDOXUxNzZIZmI2TFRiNkhiYklBamUrTndiNkhRNkVDNWM2UnpjY3EwRHpJei94c3ppZWRqSitvcDl1UitxRTFoUGlRV2hZeHdZMWk2Y1pMZUNZRVNNWXlIc1ZSWDJwUUt0cldManhnMWN2WDRkYTJmWE1MZTRpTElvM1JaTGdKVUpiQlhZZms2U3lzSVFXNkdDeHlXcUdyYVdxQzYwY1FQVllpMFYycUxHVkdrV25lS25SSEp3S2hLOC9EOXRFSG9CTGlDVWFtV3hIVmhBdGE4K0dCQjJjbHV3eDVrc1RNRllPNytPOVhPcmVPT24vaHJHd3pGMnRuYnc4WWNmZ24vd1Ezei93L2ZRZWJLRFZVTllMa3IwZXdXV2V4MnNMODZobmt4d09KeGc4dkF4ZGdkSGdIVXNWVksyYTZFaUNyNEFyR2ZsWXNFK2s3SVd4MHZvOXpxNGNPa2NMbDQ2aHl0WEx1S3JYLzR5am82T2NYUjhnc0hKQ01OSkRlck9ZZlg4ZFZ5NCtSSXV2dmlhd3l6Q21lejlHeUhaRTl1WXJJU2FLbmtGc0IvVE5hbCtKdWhlZkJvU05UTExJb2NDbEdqRzJ0aEhDWXJDRGhINUhZb2pVRUNWR2trMktMSGZZUlYrbUxwbzYwK0RCRUFVYkx5MFZHRFlkYldvcldmeGVaMDd1NFZpTWdFUks4TURPZDE5TEVSOXlYU0dnczQ3RStXc1FNU3dUT2lmdTRJWE5pN2k1Yy8vV3hnZVBzSGU5aGEyN3R6R0J6LzhMbTcvNkFlWTVRcTJybEJiN3ljZmpDR2hYVmRKVFM0OGZpcVU0QjF4QzhCNDRLN1g3V0R2K0FpejNSNzZuUmFHUk5nVHdZRllZT1VNMW03ZXdPdFhybUQ1ekNvNjNTNktvdkFGRjZXVDNXZjVpWFZKTzhRR0VNRmtYR0V3T01MOHdveGJaeUlRYnhSYVcxSDVBR0g4UkEzcFdYTXNxL0lkR3dhVUZ1bFpnRmE3QmJROEJyR0lMNGNyVEtvYTFsYWVGc3NZanlkNHRMMkQ1ZVZsdEhzOWh4TzVyU3RoTTRiUTZyZHg3c29Gbkx0OEhqLzljMzhkSnljajdPNDh3dDJQUHNiSDcvd1FQLzdSMjJqdGIyUFZGRmhzRitpVWJTd2RqOUUyQmpmV0ZuRThIT053T0hhQklxTUtCTWU2Wk1Pb0NCNFBVY2FzUW80WDR1bmNFbWh2dFFQOWFrdDRkRGpHeXBVWDhhbG5uc2ZhNWN2b3JaeEZhMllPQW9NS2pLcHBKUzdOVUpGVFBBQzFGZjFwT1hYSUozS3BBa2diYlBSZVRQT3dLWFp0QUhBVFJ5QVg3b1hOb0NBcmlSZE1LZm83Z0JiQmxFQWlMOS9STEsyNDZBanhvMElpUVJGZUlOUkE3TXd1UXVscEk0aVJMZ2szUE04eVozUWg1Y2lhUDhnaDB5NXhUa2tob21tenRFUmcwNEl0MmlpWHVsaGRPb3VWR3kvaFV6LzM4eGpjL1JELytEZi9TOWphL2E1T0E5WWMveEFRRWRnTDdNdFNOb1JXMlhLc3Y2SUVETU1hZ3lNbTdNTmlVQllvNStaeDlzV1hjUDNhTlhmQ3p5L0NsQ1ZxNjByM0tKVVFQNFh4Wmh6TUFqYU04V2lDUjd1UHNiVzFnd2NQTmpFNFBNS2puUjM4Ky8vaFB3QVpUdmx4L3RMWVU5cWlhUWNtUkU2cXRsR2phRjh0L3ZSZ25hNmdOaFBLYmFYZ1FNNXdta0tOckFZSGgvZ2YvcnYvRm91TGkxamIyTUQxYTlkdzZjb0ZuRGw3QnJPenM3REVybm9ET1Y2Q1YwNTIreVhPOTJkeC92SWxmTzRMUDQzaHlRbDJIMjdqN3ExYitPQjczOEg5NzM4YlEycGhwbTl3VUxiUkswdWM3YlZ4Zm1rR285cGlYTlU0R281d2RIeU1vNW9WLzBGUSs0ZS9hZEF0dGdaczdmd0dXeDM4eG4vMlg0Q1hObENEVVlNaFpGRDdIQUZkV1lsUVhrMDFuYXliQ1QrQlNZcGtwNS9PWDIyelo1UGRYWllqSmxNdXc1UTVCYkVxL2FmaitrS0xEcmhrSjFnYmVrUkUvL2VvWmZBSUlxdmtVa3ZzeURyQ3NDd3FLdHd0K3RvcjBheW5kUmp5M0Qzcjl5MTIvQUZCNlArRCs0MzRRQWZLVGNJSUdUMkNsTWU1MWRrbEFhV2xQSTdJU3VWWmpYNG1Sd1pqQXFqVGhSQTVFcEIxOGljeXhrbC80Nkp4QUtnQlVBSW9EV0YxcGdkRGpOb1lWR1F3Rk1FaEFRTVFUam9kbkgzMldieDgvUXJPWDdyb0lyK0xBcFZ2Z1NvUXhwTkp0R0N6ZGUxbjh3Um1ZSGh5Z3YwbmJrSng5KzRtaGlkSDZQYjdtSm1keDl6Y0ltWm5GekFjRG4xaUx2a2dFeVhBMFhiVm9iZW5aRkthaDFONHJPSVUvemdkUFpVczN4TXpqUlJieXZvL2RxVEV3Qjh4SUhMR29vdkx5emgvNFFLcWFvSjMzL29odnY3bVZ6R1pqTEcyZmdiUHZmZ1NybHk5Z2pObjF0SHJ6L2ozcVowb2k5bnpOUVR0WGdjYlZ5L2o3TlVMK096UC9oU09Cd2ZZdkhzUG4zendNZTY4K3k1MjN2bytsdXdRYXlWaDJSak1rOFVhRTJCckhJNEUrei82QUV0ekR1Q3JLNGRoa1MrTDRhZFhKQ0UvQWVCV0I5U2R3d2xhVUU1Z2dOVFRpaFlSWk5oNnlLM1FoMGg4UkNrdkdNVG12cG9lRTVPWTFlRXJPdEtmSVowYXJITUx3dG9JempyZW50OTZ6a3NLekUwVmR4RWlLNkY2ZllFOXRaZUpIQUdyUmpHSnF4cG5tSVlKaFJjTDFiQ3dsc0hlWkNLNW12aWRLdnczazBmVzJYUFVrWjg4bWE5aHdqNkpKWTZuTWdlaVdHbHcxdGFFOHBqOTkyQUVUYndiaGRhMUk5RWJkdXk2cm1Fc3Rkcm9GZ1dPcXdyRHlSZ3pyUUxId3RnVWk4Tk9DMHRYcnVMS3N6ZXhjZUVDRnBlV1VIYmFIblgxTk9xcWpsczBBeWg4djJqSVlGTFhlTFM3aDd2Mzd1R2pXN2R3TkJoZ1puWVc4d3NMV0Z0ZlEyRktERTZHMkgxeWlNZjcyM2g4T0lBOU9YS09TRXB5bXZzd3FBQVdTUk1KRWMxU281Zy9ULzQwQzFXQ2piQjZvcGtTY1d3UldhSFQ0a3E3V0J5UTl6NE1lWVFDeGllYmovRGU1aTVXNW1leHZyU0FsYlVOOUxzZDJIcUNkMzd3QTN6aksxL0RlRExHaFV1WDhOeUxMK0hxTlVmUGJyWGF2aVZ6WTB3YmlFZUcwSnVmeDQyNWVkeDQ4VVhJMy82Yk9Eb2NZUE9UTy9qd1IrL2hCOS81RnNxSGQzSEpUbkNSRFdiS0dsMFdYRG0zaGk2djR0MWJtNURCT0M0NlcxV29KcFVqaWRrS0xFQ24xUWFqU0thYmxDcmpLYVlmcDRVdlNJWVZ1VUc5MVRFai9qYmxWcmFod2t5cXluU0NKMUJRdHdIVU1DbE5CMkZjSWRxckhBcmY4WnROY2JyZE5xVit3UFVDa1dLb2JZbmRSelJ1alZuckJTd0pCS3paYlVaTUZEY045c0tNT0o2U3RDaFRhWTlNTkVUY3pENUZITDlGZ0l2U0RBR1NkbUVydFQ4dGRhQktrRFZiNzNwalhQL2VhMk4rYVJrTHN6TjRzck1GaG1DdUxERlhsbUN4R0lvUFBMR0N1alM0OW9WL0E2Lys1RStBeWdMV2hteGFsMXBEd2M4VDNxZlB1TTJvR2srd3YvOFlEeC91NFA2OWU5aDl0SU4ycDQyRmhVVmNPSDhSdFFBbnd4R2VIQXp3OGVZZDdCeWNZSWpDblVpbUJGcUxLR3Q0UzNCSElkWm1xTXFUeWQvRFFQZjFDcnhrUU9WQkprazJjZHBYUGxFMkk5QXFuaHdFTDV1T3ZJNVlNVkRVNjVNYWkwN0tQZzZrZzRPQjRQYmhBWXpzWXJZVW5GbWN4ZkxDREZiUG5rTzdaY0FBM3ZuZWQvRE5yM3dKNDBtRnkxZXY0L3JOWjNIcHltVXNMeStoYURtR3FSdXJzbytSSjFBQnpDek80N241bC9Ic3E2OUFmdlZYY2ZqNE1iNzAyLzhMSnU5OUQxU1AzZUV5R2NPd2dLd0RBWTF4bi9YNVQ3K09xcDVnNS80bWpnWUhEcWN5QmxaWmFoTlJockF6MklHaGtqditFVFdDT1VMclRNbjVLYkQ5RW4vZkUzN2k0ZzJncHJMYTVlQndMTWh0QUZ6bENPVm9sYXpCMlZQcldYMzJRQ04ycjExa0h6cjYzRGRvakJKT2QzYm92aTVGRkJPUFBJdVB5WUV2eGxvVUhvY0s3RGRMN0ZsUC92ZFpvNWMrOGNWdkxkSHRSd0VoRkJTRmt0QnJJTy9Cb0dPMmtOSjVBdHNSUGg3YlV5Rng5ZW9WekM4dGdBdUdNU1YyNmkzWXVvSVlKeUlTYXoyR1Fka09QTHU0Qkc2MXZkMlhqWUJnakRtM0RyUTcyRC9BOXZZT0hqell3c0grRS9UN2ZYUzdQZlQ2TTNqbXhnMk1Sa084Kys2UHNITm5DenVISXh4TkJOYTBBRk1BNVR6S1RoZjltVDc2L1JsME94MGNiWDRBc1JZc0prOGI4aWU2Uk4ybHZ6NEtlQ0tsaEl1dG0rK05ZWlU1bXgvaGl0NHU0a3hMVldTeHdvQURmRlhtdllXRnRSVmUrZXdiNkp4OUJudDdUN0N6dFlWSGozYng1T2dBK3dkanlPNWpGUFZETE0rVVdKL3ZZYllFbm4zK0JWVFZCQTgzSCtDNzMzZ1QvKzhmL0I1RWdBdVhMK1BhOWV1NCtzeFZySzZ1T2ZrdmgwT0o0MmNqQmhaV2x0RmFYUFJ0bjhlNGdyZEZ1STkrMnRPZlhjRGk2aW91WDM4VzFXU0U5OTUrRzkvOTBWM1VWbElmcmFaTFNWMG5TQjVWT2dsUVZQdm90U3ZpQkZnMmkxbXJGRVhYdjA2czZrNTMwWkxNU1NqY2c3cWhvQ1duTHdrVzdPN0pUWG9MZFVBVzdxYXg2c1YxMElYRXVPMHBMM09WZlI0OXpMMDlGUlBEdUFvZmxSZ1lxdDFwNk8yZlUrbVVocFV4RlRlUVQ3eXJqTHQ1d1lURWE2MjlZMUFjY2xQZ3JTZFhOQXVKWmhtUVpISVNxZ0x4cjIyWTBPdDFVSlJPdXN0TUhzenlIdk1TSXdUaktXK1Zvb3NOK3czQ2JUWGo4UmlQSHozRzFvTXQzTDEvRDRQQk1XYjdNMWhjWHNiS3lpb1dscFp4TURqRzFxTkgrTnhucitQNk05Znc0T0Y5VE1oZzg4aGkwcHFINlhmUTczYlI2WFhSN2MyZ2FMZFErQWdpMXRodllHaEt5b0luWkVFSUdadU1nNjI3NG9WTDlJQ0krc1BJQThzRWc0akZZR2JvcWtHL0FPYWw5M1NiU0t0c1lYVjlEZXNiNS9EODh5OWdOSm5nNE9BQU85c1BjZi9lWGV6dTdPSnhYZUhKMWk1dUxoZjR5Wi85RzFoWVdzYnYvOTcvaWZmZWZnY3JheHRvRll6eDhCamYrNHR2NGl0Zi9HUEFGRGgvNVJxZWUvWW1ybHkrak9YMWRSZVBwZ1Ewc1VPMktjTWhjRFFnSWV6VTAyL0p3RUpRdGpybzltWlNBUng2ZEgrL3lXTklPdXNpc3YxZzAxU0tHaVU1cFdrS0tmVWU2VEJiYjBXZmNpN1ZadzVXUEtlMHdTQ08rSUwrK2FRVTlPcGI3WlhuWDd1b0tVL0lTYVZGMHI5TEdNTkZJSTZER2ozajVETVl0VC9OalhVTHBlQTY1ZmtadGNNcEpEbWs4WkoxbTRaWWlvWWk4RDREdFVYdVR4aFpWS3dtTUp3OWpMa3NBNUVuNE1aanRjY1FLQnJtV2JFdU1OUGpHdUpIblZZSExkcUFkS3NMekVBMUh1TlB2dmluT0R3OFFxZlZ3ZXpjUEZaWHptQmgwY1ZqYlQ1OGpNZUhkM0V3c3FpcFFDRTFQbE1McW5yaURTd1lLTnE0Y08wR3luYlhsNWRoU1NZZ0ozVDNOZ0tjbUVhMEpYek9aTm9TbG9ROWhVREZBR3dBQTBWbjF2a0pDRG1NZ0lWU2NLYllPSUdSaUxaNE1wRm8rNWZFV0xOQk1NWUY1dVlYTURjL2oyZHUzSVFCWWZQT0ovanpMLzdmRHZnVGdJMjdIdCsvZDRDcTJrV0hLeXpQdExBNlA0T2wxVE5vdHd0TWpnN3dyVGUvZ3E5KzhRL1I2dlJ4NmZvTi9KMi8reXVnd3NCWXY0RmJOY1VSZnlvcDFpTXpLVjJLbndMNXRwR0lNaEViVkE0ZjBWUm9YYjc1b2s2NUZwWWJlYi9odWlTd01OSGVjNE5jb29hYmQ4TjlNN0lIUXpSYkhGc0hDajJwWVNHbHNicXZEb3R3T2dhMlY4VGNJZzljSWlNd3ZoeFpzSGVrSVU4UkZXOEl3SkYzN0J4eVRDVHFVdkkrSjhsR1RoeDZIZVA3ZGcrZ0ZlTEdlS1lXVk93MkFuaWtsdVBlMjR4TElyV3dVeUJpMWtyNHlvRDlyaGpiRGY5Z2gzUmFJWWswVFZLYkREc09hbHlFd1Joa01xNXc5ZW8xN0I4Y1lXdDNIOXQ3OS9Ea2VJSXhETGhvQTJVSjRRTGRiZytkRmxDUkluWjRySVNOVjJkYWlxTkJhT0lOSk51Y3cwTkVlbHJpZDlVUVlSNUVrNkpPYUt2TklzVUJvaEprMW1HYkNJRW00WnBHSUl6aUErY0lTaW05S0JLenNrQkxSMUt5TUw2YTlnckh3dlg5WlZGZ1ptNDJaU0NnaHFBQ0ZTVXVQUHN5UHRwOGdNZURBUTVPTE80Y2pjRFZFL1M0eHVwY0cyZVdGN0YrOWp4YVpZRWZ2LzE5MUwvMFN5ZzVaWGRKU0UrSzdnTytxcE9HZGEyNEE4aU4xbXBuRVU5V0FYNCthbHpKdHRNSjd2djJTS2VuakU4Qkw2dEhObG5KTGZHcGFaK25WNzlRZ3doc0ZUR0xZbVVya1c3Y01CenhaSC94ZEZWU0ZXQ1IrZXdwS2FPRVdpOXE5MG5CUjBtTG5QektyRElCb1Rqak5INHNJUnBRaVpwM1g3UnlVak41U0FSV3JEZWljQ2N3VzJEaXB3T1dVdklQeDEzUVJzcHd0S1pxZUJ5RW1DY09EN0N5R3c5eDZPR3pVT0QycW5aSGxPZWJ2NjhPUmJadWRsKzBXaWpiYlh6L3gyL2hVSHFvMElIdDlORHFkREhUbTBXMzMwT24zMFBaYm9OSGcyUkJiZUZwMDdXdlJGUjZLN0VxSGRPREZPZmFVN21KbVpWeVRNbWhoaDBzQllxM05FZ3FxdDdQWXFkVUwrRHlDNHdhRWpRWW04aE5ZVVA3RkJPU3lWSERwWFlKdjFMWEVFL0FxYXJLWHdlTFZydU4xMy9pSi9BcDA4YlJZQitQZC9ld2ZmOGhOdS9keGNIUkFZNG1GdHRiSTZ6dTNNRlB2djZDazJqWE5oc0xhNUpUT0N5c1Z1cFRBa3VGcldKU3NNZktPRzBhMm5MZU95OWxKYm80cmw4NFljbEw0NE1pYjRydzU4VmdhSjd5cENaZmlQUHp5S1FNRzN3Qzh4UHJObzNySlF2elNWY2lKWGM3QXA4SE9penBjRU9iaURpU3pFSEluL29ROWxDZEU1K0Uvb005SWh5eUE4aVB2SWlkZTIyUW9ySWFYM0hENkRQQ0tOWkpqeTBEazlwcHpKa1l0VGpKc1p2V2lSZFpodmRObVhDeHpZaHFPRTlaOXIxaE91VzhQbDZwcFFLTEt1eW9ndVJlREpKNGs4TDhYaEQrSFRCc1VMUzZZSjdGMnRvYWVuM25SRU5xazdWQ29FcVVRWU9IYVlSUVdVRXBrdm1vaXZhZlYwYnhlUnlWTnZiUXZWVk80a2xCa2FScUFVU2ZBQktOWnV2VVlWRTBVbkxWUDVNNkdRV1NrV0JPRTdCSUVrOUpzcE4xUkp2QWFFUlVueHAyOThJVUJSWVhsN0d3c0lMck4yNENWakE2T2NLOU8zZnd2Vy8vSmVxcVRrR2s4SUJzVEZqeXpNMFltS2tXUTJnVEF5aW1UUHRweXJkUzhVd2ttWHdrR2J0Vml6cWxWU1ptWm1pak9hdnJzN3hLS01ZcGtJWGJCWEZ4S0gyenBDM2tSTTNNSW95VU83R29WdFpINUJWUm1oZ2FIWnNIZDRiUkQ5V05OeUdLK0NKYmlXTUZTRkpFdVV4SVY4OHJSWDhFYkVQV1lPeGRWS01qN0hkZlA2NnBBMUJvRFlESzR6aVVFVlFvMG5qMUErWkdLSktOT0czYStiV2VBRUhobURCVk81MThwcGxIMnBWWmdWOE1aa2F2Tnd0VHRoc0c2dTVuYXV2Y2hvS0tMYks5ckNSbVdJUGlHVXZ3T0k0N1JUNnFLNkFwNmFYazVoTlpHRE9uNlhiR3NKU1VzRVFlU1JmT01xeEk1eVRHeVlMT2xXeDYycVVOUnJ3cGluZmdqczlKK0V4azRYUU94cjltN1lsQm5TN1d6cXo3YXNtZDdoS2NpT1AwUTgvdS9ic3hWRHRqRzg0OW5JQzQwS3JHeWxobFl3UlhYU1FiOUZ3dEoxbHdSWmpaa3lieHhEOUwxUlVqWVRaRStlWkpvbGV6TklCQ3lhUGg0M3NvZ3A2azZsdlVsS3pRRmtMNllvV2RublVzTWFoaFJ1a0JIK0traDg0eUFQd29nb1AvRzBlRld2eHFyTW9kU1I4czd1amtuWU84SzhxWWdtT0tlRnZ5Y1BNRElpRHhoQlB0Rk9MOHR4V3R1UkZFb25qWXFVK1VtSUpFbEhiZUxHSGR0eFdKenF0T0VXYUEyWEVmUktrVlJXQ3A5cXd6RzArczZJVWd1UjBrcTQzZW5kS1VvY1NpWEgzMGpOK1JkeVNWZ00wK1ZHOGMyaUdvWVdPcFk2Y2xncmMySm5NQmhBQUFJQUJKUkVGVVZ3YUw1R1lYcEpodm9rSTFmYktQSGsxeWhscXJVV00wdnhFdnZBclh3SEVCeENxeUVxd1hLVWtXamltQnVCYmFSU3Z4bEk0Q0hyOVk0dW51UjJqdUlKT01DUms2NDFENlIvSWVVVzdZb1ZPQUFxVTZhN0VrWTI1b2MxdFNkSGxTVXZwUTBtZGJxU1JMdmFqOVVIMGRleXUvNEtpVkNPMXV6UlFPZEpFY3lkU1pYWktrdjZHa0lwMXdHcERobVBHWWcyOUNYZ0hsRnl1cExIU0t2Ylp5SzdHSzIwNnBtN1MrSkF4R211N2tBRXlRNWdiMFU0c3NLREdmd3NKeDJJWjdJK1pRSG5FbWgwbS9HeFNSSE02dEJKaXFjWTJWWUVVZHp4SC9vSWc2aFpzNWlaUVJyMEpsWWNrcVNiYkc3NU9YWUJ4ckNxZFRTdWtsZ285aVdKUTJxTVVrbFp6UzhKcmxhTmFTR0gzYXJNVXBQalVBNWdNdmxGdFRNclpWRzAzWW1JZzhhMVBpQnNmUjVvZXlRTmRtZ0JURnNsdVJscUp1d2htVlNoMm96eHduUGRIYlFvRjJpY1BBNmQ1SUt1MGtZOFdLZ3ZPYXFUcVVad1RLZExiRmRHQ0lSa3JTcENWelBkS3FTa1ZxZzhyc1BLMUJvU2xDTjAvWjhTZVBnTFNHaXl6TU0rdzhXV3ZCdVFRUm9vNmRCRElsdzBtQ3NEY2FWYWtwUkM2Z01xMTB6ajR5cFdZbElhL2VWQ1JVVmxZSVJTMHhtNjBNNHd6bHV5YUtFRVNrK0FKQk9LRk9vZ2pDcVBrcWtRZDNBckdHZEkydkhYVDFsa0h4ODVJeWdRcUFvK3RwVSs2QzFWbU1VWDdMMldhUldWanJwRmhLcFZ3b1E4R1VKZ0dOYVpOa0lKQ1hMR2VMbkxLMmp0UkpKbzBXbURLamFzOVZUNE5KMWR1VG1tY3JvVkYyS0hDZ2RDbXpERXpaWDhlZU9DSmVxU3ozdFYvOFozejAyVnZOSXhGaG9JRTUxYXBtWll4MnpTUk5tcWFuNU50aDZ1OGwycVZKZGsxbDZuV1VRY2hVdXBka3Mzd2llcXIvQlRWdWR0eEVLYi8yVXoyZlg0Y0ZaUXd4ajVHU2NvMEpwdHlpNmIrQVpabk84WU5qZzdseXhJQ2tuc29wUzEvTVpyOG92dEx3Rko3TWxNaDRrREpRS1FzQVl0eFBHcHMyeWRnRUNMSXhDZUlZVHpFRG9pbWVxSkVRWmNxdWVITnNBbXhpWWkxbEpyNnhLckVoTXlGdTNta3FZcW1oMG92SXNGV2tIaDMySUUrVm1vb2ZFekxTeURBUjBkTmlEMWd3YVRjSTVDZTFvOWVpVWFsSTlnVG95SmNzTUZhMFQzMHlFRkZ0WnFyS2ZINkIrSkkrVHIvOXcycEZldytrQ2NKMEVKMVdVUG9OenRxSTlWdnZqb3hNMHU2OUFjU0N5Q3FRazA2L3pqWlhsVUpWV2syaURXVWlJT1gwZytUYmw5b3l5aGI2VkwwUU4rSG01dEs0LzVtcVB6azJhNlVmTmYrTkd0dVhFd1BsdXZCY1ZOcUFvWHlpcmJEdjR5eEZFQ1MwbStraEpFVTdrUml0RGNVd2hBNFNDV2twZXBSRWdUbkljV1FYdUQ2bUZxOHlUTm5xTmxHY28wRUhLWEVNVTVKakJORUt3QkNtZUtMRjBWdlFTT3ZlTi9UWGxEWWJ0NWVJVjBFcnBKdlQ0azhoTUVyODRmdlBTT0toUkZqSlhab2tZMitGMDlEOWVwcE1USEZHdFRPU2tvM21JblNsTmZlYnQxSUtSMnR5YXBTbDRxbW1nVGNBQlJxbmU1Yy9RY3lVQXZIaXRXWTFzZWFFWGxBS3pLQk1ycHhqSTFaZHA3UU1PRzZrcWF4WG40T0NWNlhOZ0ROa0FoMUtiV08wcGFQTTB6SG5USW4yMGZiUEdpc2RnR1QwYXIzSXhiOUg4TmNRa1ZNNXdNUUtieERKYU1GQlVrd2VkM0lLeXVhWjBhQjErdTlkNEtscDVWUDFueWVrbURnMml6MDFrSUVYL3J4VHJEdFJjUURLa0NMYnpra0JKZjR4VS9hcFFWcHN2ZGdJTENpRklNWTlSR3lkM2lDTEtZdk9YY2t1U2xmWVRLSkVIcExxMUF3bVQvK0xhMDBBSStTanZodTdkRVR5MDJuTGtVS3Fwc3dVcVZHWkEyTG1pWkI1dWVzOVh5S2dXTWZ5TndGZ0pGRHRsOFliL0x3NzNJTDQ4Q0ZTclBPT2tTS0JLMmtKT0RhRzZhRk53VEpUcHd3b2puc2xUOEZ6ekR4U3FUUlJoV2hUaXhqd0E4WFZTTmREMWFRQk93RWFMWnVxOEsxRWZnTk56U3FES00zN1hVUUVuUkxBS21xWjYyaDZ6aWRrVENZQ25Ray9FT1VhcEpIK2xLMEo0SlJ5di9rWk9lZThUUVdLU0JRT1NTUkRrV0l1NWg0RnhUUnF3UTFaYlhJRDhsU3BCRXlSZHJrMXZoYTJVZkxMMGFQTk5Gb1E5U0RxbTBYTllrOGZJKzZoWUVtRUJ5Wlh3bGhXU29uUTZ5bDBtL1NjTHFqSmxNZWQyTWJwU0pJWmtHUXV4b0hrNURVS25xQ2ZUVS9TbVVhTjhadEVHeWc5WHcvOC9DbFNqWXBoejFWbWlPR3RIRXZzTko3U25vcmtlK1ZFalk2c2c0eTZ5cUJNWXFySG5mRTFBOWVBay9rcGhRMU9vY3U1TTUzTE5QREZlZmJNV2thY1M0ZXhyRnNrbkdWRDZBZVd5ZjJlTytTQ1ExT1R1T1NPK3VCL3lHRjJ3NTd5UzVTNTZtYW1JUDZQV05MbnNXRnpDaExQcVlWS3lqVkxHa0JjYm55THhnZzA3SEtNcC9sY3BpbERTZ1hTSndNbEhLWHhtMkhqeW1WQzZYZVFxTUJLWGhBNThJRlY1SGRYejZOdVJneEdxMmcxVDg2QkRac2RxaGx6YWtvNmFZRk01NXc3QlBsSDNuOHVBUnVnOEZaR05SRXFULzZJUlllSU1rSkE0dFdIdVdzVU1EVnJvQ1JwMWVkV2VNQ3Mzd3pKVzIwSE9DR2VyTlo2WFFGRlZ5UlNoQkFpaXRrT1lWd3BIanVBN2xjcEh5ZGxJQ0tGMkRTS20xNG85WVVvcWgwbG1FWmt1dkNrTlkrYUttb1FIcVl3NVVEV2RLYW9SUGxKSEttdUNoQ0YwbWtFdllQdWpxUDJRL0VyRFBIVTZaakVYR25rRmJHSlptdWpyaFZKOHMwREpWYWp4c2dFZXBUWUJQYWE3ZGMwTjBQVU5hREcvMDkxNGtSUFBkdHpVTytVT2x3YW13Sk50K2QweXNZQmthbWZqU05oSnJkK2txMnR1eXVPNSsvN3BERGpoZzcxYUlKWlNMVGJVOUFEWGJoUms3T3ZZUXlpVEs0Wnk1YndNSGttRTBmR25rSHRnY0hhQUVYSU1yU2V1ZWhIUW82TG5iYTUydC8waWs4SFY2TFloUmlaQjdJRW5RR256NmhtdFVJVTJ4VXJnb1U1bHg4L21Vd2E0SmFMTXpOc1ZDcVRtcko0SU5OUXJoZ0xJeVdwYlFSN1l2aURPRy82bEVEVXdGb3lTemFKSTlLSUFvaGt1ZmFSK1VlTmFZWDRUVTI1RHdYaWpZajEzUStweUc3UGV4QkJiYjE5T05LSTBFckM2bGxKYWFXSnNpSXgrWks0Uy9rWVJQQk52TzhrRkpndGF2R3I2WHN3VXlISzBCQ2k1UG9jUUV5clhIcWJhZDF4TjJ1TTBKdFQvbE5QZURxRndUYzFFVUIyQkozeVExUDZZVUlUZE0vWnRxSHlMb2c1azFBR3FRRmx2aWVxSjUzZUNpTzRraGkxMmtCRTk2RFNvSmVvNFExbFl1ZDh0eVRGZXZMZ0Nua3RBamowcU5hWG11RlVJUVUxK0tneS85K0ZQd0hIWVJTb2FQL3VNNXVzMUhiQVVwMllYSkV5cTdabnRmaUQyR1Z1YmhZeml3dW94eE1jSHc5eGRIeU00K0hRWFoxSm1mcGZKSERIRk1hZGxOYmw2QVhna21CZGZyMGRBOVVRUkRWQVJSb0Ztb2E0SmMwUHZSREhYd1BLNWR5QmF5R0JmTU9LR2FuN2RnL1V3aS8wOE42aWZRZUpHaHBrVHh3YkhxS3NUbENVWGRlQUJGR05waVVLZS90d1ZxTTVhdUNXTmtacms2ZFVUK2RscDgwRmFySk1qUVVwdWtXU25Fa1pCb3VrZEM0VTdqRlRUZ25OZmVzd2JWMURDbGluQnEwNFA5bW5sN0NremVocEcwakRTUGV2UXZPbUpndHVvaWFOUFhLYUpJY3BSaGhOOVU4aVRhcHdJRzJRTDkxMWdFWmo3a3Q2QTZCVFpwWTYyY3FkWHF3UVc3ZCtqZHJsSktNcnh4T0YxYWdPQkdPQzA3aFZwNTBGVUNuVVA1Q2hTQVdVcGw0U0tzbUlKTkZ6WGJweURWdlhvTUpnYm40T0N3dnpxR3lONGNrUUI5dGo1NDFJN2tNWVpoQXhabWRtMGU1MU1SbWVvQjZkQUpNSzllZ1lWQTJ4Tk5mQjJmVWwzQmtzcHJHa1pLeGNmd3FxVmtXUVNEUUkyWThKMENUbG9TaGlNKzEvbzhOTkdJaGYrYmFwazFYM016RVRMZGJuQy9RR0Q3RDErQUREbXRHZW0wZG5iaEZGdXc5aldqRFdSYUVadzJDWTJCSG55OXRqUUFHc2s3dzEwN1AyU0tjSTRTaEtZQlV4R2dza20zdVZuMHZhZDE2dkNxMlE1TndrQVVGbXJBODVWa3d1UHJXdG9xY3VhOVdDVHhYMmpaK2tVMnVDaG81Z0d1MElmMURJYWVRR29xZkNFZE1FeGxQNkN3MjNCOUVaczNKRDBReTN0THBKeVI4cEw5d2NVTWVOeXlBMnpCc1NPS2ZBeEVENnN5S1JjaXd4VDVTaTkzcE12TFNBRktvWHBtU3RaWU9FMkwrMUVYWGNlUDUxUE5XUWxGNVY3ZDdiK2h3RklrS3YxMFc1c0FTU1E1RDNnQzhMUnBkcWpKODh4TW51R0VVOXdwbkZXV3ljVzhHWnRmTlltSjl6MlFVQ1RJNE9QYy9DZXhVbzZsRUNGOUl5Q2dJaUp1UDZkekIwVUduY3VLMW1LaUphWWxuTlExZHBOb2tLeTlsclFYUmFrOFhORzlmeDhtdXZBOEk0UGo3RzF2WVc3dHpkeE9iZGo3QTNtS0ExdHdnQm9ZMEtob3FHMmJXeTl0REZKek1NTjNqK1VaaExjVXBGV3ZXWEVZN1RoaGpmanhScEtWS1B3eGlRVmY5c0c0dHNlcjRlcWxqcjNYcUN4YmNvZzVBUUFKSnM3clN0WGFDTjIwYVYwdkRIbEp4ZE85MWlpT2RGcEhaU0l2N0NyZ0pJWWhpS282SDQxWFRjY1lnNmlxNDBpZFYyR2dCaXhYcURoem91VEFHaGlaV0cyWG9JU1FpN2xsVVV6dWcwWTBNUDdvRzRBQXFxSFMrQVRYRXlUQklORWdKcGg0UzlRNDRDanJ4OHMxYWtpYndDWWtkdUVqL0hqMjR3K1J5TVFPNzBsL0JlQWUydkhKR0pDVzFXSWhrQ3F2RUpmdktWcTlpNGNBNnJLMHZvOTN2dXl0UVRqd2NBMXRab2xTMy9RSE04SXpJNmFMTXFEdDZJUU5RZHBCK3lXVEZIS3NHV2xFOEJUWldxbEJoMitvVGh4RkprNWI5ZlZiVTd1NW5RNjdWdytkSUdMbDNlQUlFd0dRdjI5NC93NE1FRG5DMStHaCsvLzU3elJBQkFkWTFTYXVmcTdMYmR0RmtIU2JreTdxQk13SWJUVVhIb0NvZWlQMlZ6N092RVJCelZlMElhak02RlRRS1pPbUlwMHFOWVdkNnI5aVNqM3lrbVpwam5oelNzeUtQZ21HaWRDOUVhbE8ycFJsdThkMlREdU1TZkUwVVVwMHdob0Y0MUYxQmlmVkVwOWQyaGh3NWVlMDFpcENqUlVOcEVLRDRjRkN5TkpJMFlkV1pkNEFmcTZpd1FIVUxwcmNkRzRUNFlQOFlKZVdnQmJaRVF0VXp1NUlYMzhFdm9xdlZnRk1lYmtmZE5sSS9yVkNYb1JvTTFySGgrdWxSQVhjSFVBcElhazVOREhCM3VZM3kwanhtcWNlMkZaMURiR3N2TFMvamxYLzQ3aWNaSzdHK1FNNzQ4T2hyaTBkNWpiRzF2NCs3ZFRkeTlleGV2dlB4Y0lrZFJQc2xnVWZzQUsxNEFxZkFIUWk1ckpabWl4VVpaTkZJNmIwRE5ROUp6SkZ1UllpaElLcmJaR1B6ejMvbW4rTWJYdjRtcnp6eURLNWN2NDh6WmRjd3ZMc0J3QzZiRDZIVDZXRjlmd2F1dnZJaTYvZ1YvYUZRNGQyWUZYLy9XZDNFd0ljd3NybUoyYVJWRmJ3WTFHUkFiTUFIR09EZnBjR3F5Y3RnTlhwUEpEVHBWYmFRbHlsQTJhYVMwQ1Vxd0ErM0EzSFRNVmxzaVpjQ2JlcDBHR0llbnpmMUp1UzQxUnJNT3I1dGFZVmxZNnVsWVFqb0lTTm5DT3kyQTFSYkRBY20zT2Z2TnMvRnNPT2xzbUhtYUpERHdMckZJUGhPK0w1ZW9qNDdlUUY1Z1F5TEtMNGlpN3A2Q3Q0cTNJWU5vSWdabGdRdGlKVHJGeGk1UWJFWVB6dnBrUC82QTczZXRNdnB3K0NJSEk1Z1lSVWI2UVFrT3hLb3lTamJ0dFc4UExOcDJoUHJKRGc1Mk55SERReXoxMjloWVhjTEs5VlVzTDl4QVVSaEFhcGRJRk1hUlRCaFB4aGdNanJDN3U0ZUhXenU0ZTNjVGc1TXhhaGhZTGpDcUJjT2hpMXN6OFdpSHhzOGpnODRxSUJTa3NuOGErUUhwR2FHc2ZadnVHWk9jMkhwTDltajBJcVNFTEtsaUFobnNEUmw3SDI3ajdRL3ZvNUEvZ3lHTHMyZldjUFBaNTNEbDZtVnNuRHVIeGFVRkZJWHhxVXV1SXJ6KzdCVmN2M2tGbytFSWozWWU0Kzc5QjdoMzl4UHNIWTNSbWxrQWxWMzBEY0NWeElWSlV6UmJQV1duekNCTHJUbGxjSXJJSDNDOEFZb2FrVFRwT2NYYjQ2OFk4eEZSQTJGckFvaW45L1UwM1ZoUEEzcXFtdENWVFBhN2NzclkwRGZQUlMyWkVnaTFIcjBKWlcxQUtpbVU1Uk1sSXdyckRRdUNDaXRRZVNsWU0xSFMrRnRLVk15NGtMMGVJUG1aUzVRV0IrS0k2L3M1anJRMDB6QldKYVRSZjhsZFhWbW1mUjZRck1mSkwyNko1cGhKektORFJ3VEoySUFrbEZRV3crRVJMcDFieGNhRkMxZy9NNGZWcFVWMGV6MlBMOVl3YkdDTUFSdUdyZHhuUGg2ZTRJZmZmd3ViRDdid2NHZlhCV29VYlZSbzRhZ2kxTlJEMmVtaVBkUEh3c0lzRHU5Um82UmxOWEZocFZ2WHJFdktEQ3FnWityQlFveForUUdrS0hkUlNUL2h1YUFZZkpEY2NxTDVSaUNKK2RQc21WYytqVkYzQlp1YkQvRm81eEZrTXNTVEIwUGMyZjRPNkV0L0Rxa242SGNMWEw5NkJWZXZYOE5mKzZrMzBKbnB1eEtjZ0U2dmd3dVh6K0g4cFExQURLcXF3dDdlUGg0ODJNSHlUNzJDMngvOENPUGhNQzVZeWdubGFxcWx1UHBlUHQ0RWpYUHVDV1dPUzdFS2xTYjkrcFIrZkdyaG5vN1AwMVQ2Yjg0SEVBV1lOMTBGdWNtdDAybWtXcGhGMHpCaVNBUXRhaFg0U1BHaDBSeEhtNlg2UWlqUE1jOTRHT0hrVWJ1T3BOTVNRcWhSWjRMem5OZXMwRlpWR1ZnZlowVHg5S25WN056cnRhMEtUQlJkRm9rU3FVaWloQUtRV29XZGlwWUFJekxjV0N3WUppbnFZdHdlNWJaY2JoYUhjeHNiK1B6blA0K3FybEhWdnNlMkZkZ1VxQzN3NUdBZmp4NDl4dWI5KzdoKzlUSXVYRGlQaytFSnRoL3Y0ZDI3dXhoeEI2YmRRNmVZd2N6OEFzNHNMR0oycG85ZXArMU1NVkJEOXJkOTVTTU55Z2xGNld4U3Vuby9SNUZjRUJMdDIwN2hpU2V1Y0dib0VjK3hZS0FaTnM0SWcxRHFYU1ZacWl3dEwyUGx1ZGZ3NG1zTW1sUTRIQnpnMGFOZGJHMXVZdXZoZlJ3ZEhLSXpzVGg2L3g0ZTN0L0VwMTU3Q1oyWkdYejlxMS9INDBlN3VIejFLalkyem1KK2ZoWmx1MEJadHJDMnZvNjE5UTI4K3VwTHNKTy9qaTk5OFE5eDk5NDlWWTdYY2Ntd3dtYWdYS3pwRk1lZjZIQ2tERThGVUtHcXViNU5rVzVqRzBSUE9kcXovSTBRdWRlY1BEUzJpdE00QncxckZVK0RKcURwYzZFcU55R1Y1YWphazJLczNXYW5oQ0kyN1lVcWFqaFNRelgxbXZKY1BsTFJScEtCVWFLdEQ2Wk1KVVQ3SWthcHFIb2ROVEVRUHdXSVVWV1N6eXNpT2NPdjBLU1VjdTgrOFRGUUpNaytURXNuZzBGSllGNkZpVVBvSGxrcnNyekhvZUVDdGJXd3RXQjRQTVRqeDQveGNHc2I5KzgveE83ZUFZWVRDeFJ0aUZoc2JKeUxXQVJ4QVduUDQvcHpMMkZtWWNHUmhJZ0RVb3ZhYjhRbWxOcGlHNk1uaWZGcWFicVN6QzVDOGNPcUJ5QmwvMEYwMmxpS0ZNQ28zVDd5RVdoTzlHcTRGRVl6WlFjSUY2MFNpOHNyV0ZwZHcvWG5uc1ZrWE9Gd2NJejdkejdCZTEvL00yeDBDa2NXSXNiK3dSSCs0SXRmUmNGdndwQmdiWGtCVjY5ZHdmWHIxM0hoNGtVc3I2eWgzV241Q1FlcnlsVUQxN1U2VWtSTmQ5VEdKOVRnM1Nydi9sQ0pScjhKYVl6eEpPWnFUbThtdWc2aEpJZk9XckJUQWdCOGhjRkl5ZDFFK3BDVmFFTXFhZ3lkRC9SekdySWVVVEo1K2pzUmlrbnR0M0NmMVpwUmdpZzQ1VFlDRGRWVUlIcWxpejRabmY0NlhYbmoxMkppeTVIU21tVjY1ckM0YmVwclJQS1N5SzFuYlU2aTV1SFVpRWtTOFJKbHdEQkhjMG9Td1NTNVg4VHlQclllUkZQVDJoQ0Y1UytYcTB4aXRKbjczK2FEKy9qNHpqM2N1bjBQQjRkRDM3dVhHQXZEb29XaTE4ZlMwakxtZXkzWVlLd1pxTW1sUVc5bUJxMnk5SWk5eTFLb1BUN0F5a2syQ2hXSnNqNDNqQm9kZTgxTlJOaFhQRFZaZFlKSjFHNEVMWU9WWEtTbFpkV1NtWk9rc1dnRTB6SS9BczQ0R1lMYVBUZFdYSXNwRk4yY2lCbHo4ek9nQytmd2JyUUxNeUFZbUxLTHBldXY0TjdXTGlhREF6eDVWR0h6OEJhKytjUDN3WGFDMlc0SEY4K2R3ZVZMNTdHM3M2V2NyWVA3RkNXalc4MFlWRDI0TkJnSDhjZmNyRGg1SkhwY0tqd0lUUG5ZUFBEOG1WTEZHYlVjbEZ5Y1F5VVptYUVFNTVOQjJyZkJKa3E0YmdXVXBXblNpQ2haZlRSVDR1eStKUzZkeEJZdWVIZ1VWYkM5RW1lbkdGUjBaS0Y4eTVxc1EycU1WNmJOQ1VURkhDVVNSZUNTNjc1S2RXaHFNMGt3djkvTm1uVEo0RHVnWFRERVIxMWxiZ01jTjhEYUp0ekNQWVFKQ1k0VkRxRVJuR0VUaUtRYU1mRzdLS21aTThUbC9YM3ZuZmN4a0Q0cXM0aisvQUpXMXRjeHY3aUUrYmtGek03MlVCUUZxc0VUa0gyYzVMb1dQa2lWa2laRFZVN3dObGhXQzBJYUQzUE81VkJTYUFKWTdIUTlLc2tqTDZiY1VENlhoc29NVEtHanZwUWd5VVpnQVR2UkxyUk5nNDlFVkxMUlJLVFg2V0JTK3BHbWNMTGZLZ3Y4L04vK1JWVFV3dEhoQUk5M0gyRnJld2VibTV0NC9IZ1gyOE5qM0gvL1B0NzkwWS94MmdzM3BuQVB2V2x4b3grWGhxZGpJbzRwOWg0Um1MMG5BWEVrVGlXVFRadnlMU2hSNVpQUFNPQkptR2dTUXFSdDZVa1p6d3FZT1JxSmtNWmlNclpzd3pFb2ZGNnJjTG1RMjZHeVBsbVF4c1pLT1ZoWUtLZzhnQ1lTMkY0SjlVK1JUK0VtaWE2dWsrUXgwbkJGRHhTekhTK05Md1NKcnVaM1J5VTBhV2c1NC9pbWFjRkVUSkduYitOTkZPVHFrRlRpVzJ2QlFtNURFRjIycWhaSGxDb3dqSUJzNGlQUXFkb0ZRcXZWQnBrU0ZYWHd1YzkvQVV1cmErcTBkditzeEhxVFU4cG14TkEzT3JqWWFsbHFjR3RXMUlNazFiVTUzMXNrVDY0bEI1MlNXS0FoYjBxNUFKck5tVHorRXJPV281YVFZaklScVUwS3ltQkRjVC85eURBTWlzTkRiNGhRbG94V1VUUzRwNXpHam5VTjAyWXNMTXhqY1hFZVY1KzVEaEhDOGZBRXQyNTlqSy85NlorZ1IwTzBXbVUwOFVoNGhtUmdIalgwSytuam5TWW9TdVcrOGJ5U2JBZ1h2QnN6SW9XMmNHWjE2b2IwTGNsT1kzMHJTRW5xazRlQWxxTnoycVNGbFRMWkwzSVdUd3lUelBnOFJ2aW9VWEU2T0FsRlNybXh5dUpZb3UxM1ppalpRQkREUERoU1N0bjd4TVgrUFVXS1N5UGpJRnlYT05hVDVFWnNvUXhDRzZZZU9lZENXWDJGVDViYnIyV3FyM1JMR1JXUzl4NHArNkhRUWxwS3Z5SGV2QUxzSFdyalE2Mjk0a0lKNkloSnhoaU1oaVBzN2U2aWJMZFJGQzBZby9MYXJFdTdEZUNybHNJeUcxOTVTRFJ6cEdneVVmbjhRY204RFVTMDg3RWFlUVZXbTRaWEJjcXpJYkVId3lrblBzd3lUNVZONmtKcCt2WUZDUzlSTnRJMWdSbml2MHV6UnphR0hiRkprWHVJVXM1Zk1MZVF1bzVqWWV0NUpZVmhMQzh2UWtOYkpMNHFpendUNVJqVU1FZ1dGYTJ1NzErNmlweitqaExISU55dm9CVmdEUTRqaGMrU1p3Q21WYWN0MHp5UG1wWDVja05vMUp4SDVKOCtQVE9KTCtOdCtwVlZYWmpHY1ZZVjZtdGdsUnBRejNtRmtJY3E1UFpEb2s2L0VFaXBua1RmdzB2bWx4M2pvWlRVUHFqUUxPR1VZVWd1MVEzR0V4ekxWR1hjTGFTWW5KUTU2bERHaDA1NFFiQ2pacUdjelVPcHBMZHhuT1Zwd3BKY2c4TEVJSk00eFN3Vmh2R09PdGJXR0o2Y0FISU1Ka1pSbG1pVkpkaVRyMGhjUlJKODZZTnBackpLejhsOThjOHBOd3dsQ3FyM3dPL1B3Y0Z3MG1tZXAwRG4xS2xEa09QY3hIL25GQWZ2Sk1RcE1TZWxMZWNJZE9pRnJlVDNNa3dSeXBMZHFaM1kxTnBKSDhsZTFZV0toSTArcVNXZENZd2hoKzB3VW5tdW55VnV5R1ZEOURhclBNcU10S1BuS2NFOFJLeTZ6cmxKcUdTbktqVjBnQks1RmFSeU44SW1SV3BreCtMRzNBcUN3blQ0R0RMRGsxUjBoY3BCOFhKSm8vLzZBSlNvYmdTQVFrUnlEemZGL2RLWmM1cHhGSjJBdFdGRzA0eUYxU2FnQ2tMdFBzQVJpSkxjbVpRb0M3bW85UmVtNllwQkd5TWtDeThmMktpU2pTUlRGU0dLVmFReEtIWHBRS0tpcnhPTm1kQllWLzcwc0ZZYVRNRndtanN2KzZDNm0wekdHSTFId1BBUXEzMGJLNXU2dHJEV29pZ1liQUJiSmV1c21PNFRXMitLbFVvY1EvbkFVNG1LelR3dUxRS1l3YVU1b3dwVG1ocm82eW1OR1RmSWoxdlRWcE5POTJTVUdzaGZnUzlpZmY4cGZsTTBodEF1UzBYZFRROTJHcTJLc2pRWHhkYWtGT1NpVmFGWno1dzdRMnRBUUU3eEdNeDBURXB3UnY2VWxFaEI5NUYzMWxXSXdhWXJCbk5tSVIvYW5DVjlWc0NDVEtpVUpTcityQ1NyV0kwSnhIRnZzSnpMeElpY3lFdUtGUnBhQm9uVERHMldvMzBneEVXRFpidU5VQ2J3b2t3b3JMNFpuU1l2bEtuNlc4c1pLWnB4SnAybXpSSTgxSTVNNmNGbEJkeEZseUxrRzQ5SXptak1BQ3ZTQlo0cWhheENkcU1qcHlqMUZ1V0RYeUNMbGRLY2JLRXBrbUtLY0NxQ256eEY3L2tRaWhLK1NTMFdsZFJvdFZxWW5lMWpOQnFqbmxTb2JCMkRtNmxCVXBIbStFbEZWaVdmKzJRMFNrS25jVkZQTjQ0VURZejZjanlZcUdoOUNEVmxzR21TWlp0S05XY2xnRTY3akE0alJNaTFJV29CTVJGcU5Eejc5Q0NYZkphMENyUk54Qi90MFU5VFJxZEsyYTArOVBRb0w3THlqSm95c3JZWjF6eCs2SVR2TkNFTDdiRW93RHNFV0lsSFZUUmJrTktpSnBhWXhLUXp1Nm5oWkJSRGZDbndXTHo0TFBWcFNzQVZuSzhGQlZPYUdXcGdMK2NFVUs3NWE2VGhZT3IwUUl6ajBuaThOTFNJbVNVNU4zdnBuRGhCaXM2S3AybWVrOGVZcDZXcUR5b044b1Z5YVFuZmw3VmlMTko5YzJzVDFzbkFvajMzS0ROc3BNWmNuRVBaUlNuY3NmYUVremlhVklFb1pjdWdMQXVJV05TMXhYZ3lncTBzeUFhT2ZwMnN6VEh0ZjZmcHdaYWNPQ2Vpd2htdExaVEtBc21mcVVSN1ZpRXNFUS9SMVp5V2NtdmxrRUs0d2M3QnFkdHV4WEFWVlVBcEFWa1NUMldJZkVQMGt2WEh3c3EyTHZrV3BPclROb1JSUVdtbmNSTVhTcG9oNjV4OENhT2VoV1JLU1JrcjFpWk5PSkxtUElCSEpyWEVxdnpKRk5VS1MyeWFlQXFhekQ1SnJpZ2ttUWxMVXRlSzBqaW8zZG5mZzBMRGtBd29SNWRUdTQrbnJENXFKSkpTSXg0eTlYNlFhVkdFcHZPZXFwZldrdXlHVlNCUjdoTFQ5RDdSRzd0MklrNjJZZWw5ckFpTVFEMmFyRVprbEtpMkFlMU8wVWJPa1VkeEdiTFdLY0F3bEFJcmJBd084ZVBNT1BaVDZnZ2lFQnNVekdpVkJhUVcxSk1oREp0Y1NoMlJQa285dnZLanp4WGlLZTAyNWlTUUNwSU1oQ3lrcWlVc2JGMkNzbm9mVm1pNmpxTFdwdzhUb2QwcVU3S1M5WGlLMzZRNWdxQ1VKU0ZURm9naGtHYW9DVFVjZXVKNjBKOGpsTmFKS3hHZUIydW5iZENoU0hHaU5TVFVxSFFvQmJaU2MzTlN3TE5HQ2FjOGZVaFhHam9mUTFIZGZTdVl3YWhOaW1BMkx0VGFZalY5Q2M4Vkk5djI0aGUyZ216NXlwU29RaXUwdmNGbFJrNUE5TGZUNWFQR0JqUVhmZHJoRlpoSzZORmZsbFZiUWxPVG0vaGdCb1llcThpeGZMU1llbnhRaWdHbkJ0VTNhUHBaQ1kyY01hcmVoQlRDcWx0dnZ3QUw5c0hVdm44TUhvdWs5T0dPTnlYUnVES2M3Qmtia1FBVWpGYW5nMWEzazVCOENSUXV0ZWlzUk8rRGRDQnpZOU5OVGplYXJKSmFOOVZDZ1JTUWxnZTNrSzRjMUtmUTQ5S1NHWjFXeXlzZHFXbU1NMzA5RzBpNFZiUm1LSk5MMndpQzFRc3h0aWt4aWo1VmtrYlJnRW1ab05BVWpWYW1iYjJrNmM5SDB3NDl6VXdIVmNpSVZxWlJRd0dvMTV6b2lWZHorcFh6Y3VLbXJaS2paY3BlSk1jSXdub29walZLT1ZzNElKbXhENGR6KzdYS3p4NE4xUmsxMjZyc0NkRlpjV21vcUdmcXFaOUQ4dWRuWkgxWFhQQ1MvN2RvZktEaG5wb2xYY2NUa3FadHpySjdSRm5hMEJTUlJ0Zk5RaG1LN05vYXpuSlczSUl4em9SVFdWMkpweEtIREVTcnlFbnhxZ1R6RVdaZHArZEFVSlFxUzJaK29mOUVKSkM4U1dVQ2lxSnBVK2IxT0pWbXE5QjlzZmttcmZVU0VZbG5SNmhKVWVPNTRSd3l4cUVvSnB4QzluVktjd1RxYk1ZckVXVmhaak1XU201enA2bTcwVnNpdTI0NUp5VHlXbVE2ZkZYbmdaNWFNSXRHMEpJV2hUTDBYaE5uVXdBSlUyTXhpejZJRXRrdVlFUXBlRlFCNzVJYmhFckd5eEVVOUJReklUVDE3ODFkTHROV3F5UFRrVkZ5QUFBZ0FFbEVRVlErSHFPUytucHVoRnhRem91T0dtclZ3eWNQZGNrTTU1dVNTMkd0QXhBUDBMQnpLejdOUUwwNXJmQjAyeXdPa3lodnlFTDdJZE8rcUtLanBZTVRiUlNUZU5nKzNFei9NcGExS3BGaSt4VkFIQnNlZU5MeTFUUjNabllDSDZzcG5tR2tHQmlWdXZLaTNETW4vVHdsS25OR1JzcWozNEpwaHFqVExJckdtSUJhZ2UwcXNzZEZzeVhnTnRZUEN0T1RrRnNZU0Y0TnUrdWN1MkN6bGk3THBwU1FycFRDVTBNWFlLa1JtY1c1Q1V3VFpOU250a1RjU0E5V0pJdG9TNWJ2eWRkaTZ2RFg0R1ptOVo2dUl6Y0VRNUpGdFNzV3JpVE1TcVpreFZyVFFva3FINmo4b2YwSnJzREVPVW9mWDFpVWg1dzBBQWhCSTVZNHNkVzBaNTRqTGVtUVJVNXplWThTYTBTZGxJMDBWRTgwN1hzbXVaUVllZitqOXlMUm9GVGpzNGVTS0UwY1VvU3o5clZQTjl5ekhrbUZjRGFhbUV4YkVoNE1EbmJyVGFOejNXK1M2bTBsSnRqWUVQUWdidjV1bUgxTDRtZzJOczcyR1pubVRWSnQ3YVlPTm1vSlJKcDIwekpsYjJIRHd4S3N6RU9sQW5iT1RXR3owVE1YYXVEQVlVUDBjZWsxZ0hyS0hTZm9FTHlQb1V3M0t0SElwUW5naWdlUEtWbWlDVkdlRUt4ZXphSlJpZ1BxK1ozR25nZ05IVUdqSmMxbS84RWtoMGpGdXluYk1FWms4elZkZ1BJTkkzdzNqdG9aVXNrL0FXdmhNRzdRMG1GcUJMdVNORUQ4SmxqaWcwRzB5VkY4dVBWVzNWU0tVZE1BZ1RKeFpHcllrY2dPMVBSTnA4YnJVa05SbHJ1eE5PbkIrWFJPRXZvYkh2cGdCVTZOc3RaZlRNNUVNV21tcmtsTmVUaWtaS1lnRWgrMk5DMFFCVHdHcnJmaHRMR0l0eUlUcFZHUWpLbVlFbTRaRmtJTTQvVUlRdm9lOFJRNGxsYzRhc09pVkFZSU5CRW9wQ3lwKytBWGtwM3krZzlYd1dUVXNNUTJaQlhSVHNrd2hOSXBMTTF1bWZMMFcvRThCalZZejU0dlZndVZVMWNiZDJXSlN0UEdVYUYyR1ZIYys2bHNDdVNUMy9nYzBta3VPMzVEWXFVbENaK3JzWDlSMC96anFVR2ZtUFllMEJrTFRST1FUTEJ4eWhwVml6eTA3cUd0MFAxWllUeDZyZW1UK1FnbkgvRm9lbnhUbXh5UlJqVmFpeFZHRThUSVdIalV5QUlSOWNlS2VxdU1FUG0wZGt1UVd5b3BORFRtc3pWcERKUWp1bENKd0NJNjJGTHlXYlVyNWhNdFUrTVBzWXhsYjJwQjBjbzY5T3lXeVZONy9VMjNEcEcyTmdXTk9tVGMrUThhZndJWWNrSW9VN0FYcXJoSFQyaWFuNkhUa0VQc0ZhbnBRN2loSXRUWUxQVjBnUlJlb2tkaCtaaUtSSmx4UkVOTjhuNTBGb1dCbzVjSDRNNmZ3cldDOWFOYm1lcmxTZkpGbE1oR3ZyVDNtQWxsUFgvVDFWaFJZOUVNM0pRR0Zwa3FYOUgwL3NqTjU2bkZScXIzcHNiMFRtL3lsTTlhY3MyYnFtU1RMa0FyYzFVYytTbjQ0bE0zRlFWNDZsRnArSUVDRW95N1ZCdXNHaFlObjV5cXI0bHRnMXJMeWhwN2lvUTlKYkJ0a2pBdzlVV0RHaTRTZ1o3eVd0bHVLVTEwTnBWek5nT0swdzV1UWRFSnRobFdIZHlScEhIeTVhaXpYOXllNng4RlRPUWswY0dBZzRoQXBYR0xnNUxrV1d5RjJnZElpazhNNXJCd3F3bGtjb0tUNHlNY2JUOEE0K1VvRU5McU42MUJ6OTFpS2JaRDFFeTFKY2xtcnZsSVZkOTBtNFJoQ3F3U3NhNmRVeUNCNi85ZE5mUG8zbTBzcjI5Z1ptRUpyVTRYRXk1UUM2UHk5dXpFQnJXMWZpU2FyanNUT2J3aytqMVFDallqaVdsRjRTa1ZTdnFVcHFPQmpSc2NOMEphOGdXVjlBNUovbTc5aG1ZYnBUZzFSRWM0TFRCYm5rTG5iVEFjd2thV2IzeXFkVzNFSkF2K0tsdHhUSWRyaWNZMDBub3JhT29WRzVuM2lqOHMwd3JQN0VISVZOV0tjM2xhNHA4MFNobzVOWjRKbWNISTZUK1B2N0trb2xOdVNOQ2VaTHh2MHZRd1JPRExFalV1U3lMQmtPUWxwNmdmckwyODJvbzNIeVdKU0RpQlVCaUc0UUxNSElFWkZzRFlDbHlQTURrNXd2ajRFSU85eDlqZjNRYU5CbGhmV2NEWjlYWDhqWjk1QSsxdUo3ckVoazJGa2JJY0lZUmE5Ym1SR3EzVWhpTE4zaDlBQm9jR3hKNGh1WWx4eGlDTURIMUdubXhNd05MS0l2NmRYL3NsM0wvL0VMZHZmUitiRDdZd1FZbjU1WFhNcjY2aHQ3Q0VZbTdCZis4cXZoL0lPTDRER3g4RzR2NnFVczlFRFRUc05yUVZTWU90cW81aUNoN29STk5XWGFMQXZldzA5NXRQMFBZTG5zYlVQOTM4UzZiT3BPelBHYWx5SlhrS0x2L1VFSkduTElCVGZwOGFoMjJSdmFBQ3dHUnFPNkhrejM0cUVhaVJPK2FsL3Fla2laMHl5M3pLREVVanlLUjU0TTM5VDZZdUtEM2x2ZExjdFU0MUdVR3BEaFZ3STlQb0tqUlJ5Qk9CU0NYcFFDSG00amNTQzQ4RmlNQ1FnU0ZCWVF4WUNFVlJ3REJoZkhJSU16ckFyVzk5RlNYVjJGaGJ3dm16NjNqMitoa3NmL1o1dERxbDg1bTNIcVMxNlJvd203UmRxN0JWemJaamNaK1RwRUdyamUyTlRJRmdnYnpGMEtUS0JEaW0zaklwUUxYM0lBRmdBNXc3ZHhZYjV6ZndtVGMrZzdxdU1Udyt3b09IOTNIdnpqM2NmdnZIK082RExWZ1VLRWVIbUF6N3FLc0taT0dkZ1p4czJIcFJrRUppWWx4WGpQck02bUhTT2lnMW5XRkYxQkk4dmFTVVJKOVcweUNPcXNVR1I0WTRQNVJ5SmZtcGl6OWpva0lpMlVwYjVmMy9jZkNBUEY1TUg0N1VNUEtRQnZTU2JRQzZkMjBHRWJJQ2NhVEJJeWVSWE9zdmFKeUkrZ3JrQ3hWTnhSTWhpeW5NNEdybFZKdk45eHNYb0JFbUN6eE5Wa21Kc3BLR3VTcTJPN1o3ZFNLZ0pFZHRqMThrNm11UXNRYlZvZzB0Z0hnREZDc29BYkN0TUJ3Y1lIZjdBUjRkNytIZXJSK2pHby94OHZNMzhmbWZXY1g4L0FMNnZUN0ExZ3RFVWc5dkd5RVFST3p0eEJnY2JjNzlReVJvRlBJMkwxZEoreittSGwrckhKS1RzbTA4aURiUnJjaUNtVkFRWVVMRzBXZkpSSjI2aTFrTFkwZVg0OWp0ZDNIbDJoVmN2WGJGalM1cndXQndoTWU3VDNEdnpsMTg4US8vQ0VWUll1ZnhQaFl2WHNmYytqbDA1K2JCeHNSY1kwc01XRWxxUUdZWU12RW1CZC9IUEpUY09lZ0VOcVo0bzQ4c1Nsek5mQVhTcUllb01kdFAxRnBwUG9pbmdOZDVKYXpjcU1PTWhWVFdJNUpxY0tyV2FBS04yZlE5UWRkVFZUSk4vM2xob1ZORWxlVXptczZ6T1ExUmpjWWpPU2p1ckpLRU1DTFM0RFFyYzg0bTkxd3YrRXdia21QSUxpV0lHaHFBNW9YMUk3NU16cVNaaG9TTW9STkhaSlFpb0ZWK0hpU01xcUMwNzJsS1Q5N05COFFvQ09ESkVEd2NvQjRQY1BCa0Y0ZDdqOEdURXl6TXRIRng0eHh1UG5NT2l3dlBvZHZ2Z05uQVdoc1ZoWUxhMDJjZFRpRFJnSk96MFNnRUdJOVBjTEMzRHlaQ2FZd0NtVHd5TFZvUVJiSDNJUi9YelpSWVQ5TlZVMzVLUm81OTVENzRDZ2cxQ25LTVA4T01yUWYzMGUvMzBPMzNZWXAyVEhhMlNDYWtISk9OTEF3Qjh3dnpXRmhZeE5WcjF3QXdxc2tFK3dkUGNQL0JGbTdmdm9OM3ZuVUwrOGNqdE9lV3NMUytnWVhWZGZDNFJvZmdycmVQVjR2QWFrTHZrcURKeTd4WkVxL0NrMG5jaGtES3Q4QXY4anE0VjBlMVh5N0phYzZkUmZJZzNEU3Z6Mmd1ZWZVVVZiR0pqa1hJaHYrblZNbWtKOVI1dXkzTndYU3FYbTFEMGwra0ZDdzZCVFdrQkhUb2NVamdkMFBSWmlXZnpjZElkVVZuaTB0UGt0ZjZsSkdmS0hlZ1dCTGxZSmJHcnJLQVJVbzBaRkFNaVlyYzhGQWV4WGh2ZGJwU2tCQkgzakRIL0RrV0xSNXdKN0dsUUtWTjJmQ3dOWWJIaDJqTEVKZG4yekRiSDJCdGRRVXJWMWF3OEtucjZQVTZhTFZhY2RRWWR1dTZ0bG5KWnExendoRnIvY2dQcUd0Z05CNWhjRGpBNDkxZGJHL3ZZRzk3QitQQkFGUk5NTmZ0WWZ6NE1ZcW9yYURJTGpNUjI3QlJ0eUJaOWFSdHhsUk9IazdwaFVVZ1ROR2xtY1NpSUlJUndYQjdDOTJaSHY3b2QvNEpiR0hRWDFyQjJzWjVuTDk0Q2VzYlo3Rzhzb2orVEJlbU1ENW4weWMrRTZ2VDAyY0FsUVpMeTh0WVhGbkJDeTgrQzRqRnlmRUFCd2NEM0grNGhjM051OWkrOXhEclBBVHNDSWVIQjlFWGdUSXl1NDNjaDRoVmhNU2ZVRkVHeHpjVmdlYk1VU2l4NVZteFlTVng3MU1WS2cxSmNlSzJ4QWttSXd2OVFCZ2xpbEpkNm1zZFhMaUVzdkRjb0xBVnlybHljUXp1LzRPMStWTVFjR2xwdlpCdkFScTdCNW84QWttekN1MnpsaU9oVU9vYVVxbXg2YlhZejRYVDk2ZElTWlNHTWcrYWl0bEVIeFdTR1RUN3VSZ2tENG1JQUl3b0JGc1VXQ25XZVdNU1FXd29DVzN1RngrWWpWR2Y3Y3RFY1dZZW5WWWJWNjVkeGVMaUVsNzc5S2ZSN1hhai9aaU9wZzRsdXFWZ05SV1lpSENPUVNCVWRZWER3eVBzN2o3R3p2WTJ0cmUzOFAreDltWS90aVQzbmQ4bklqTHo3TFhjV3U2KzlPM2JlemZaVFRZcGNpUlJvbWFrR1Jzd2JGZ3diQTlHc0kxWkxNTWpqUDJmK05IUGZqSWdER3dNQkkxSEdtazRFa21Sb2lTeTJjMWU3bDYzYnQzYXE4NitaV2FFSHlJeU16TFBxZTRtTVAxQ3NKZGJWYWN5STM3TDkvdjU5czk2Sk9PcGpVQk5ZdlJrUWpJWXdXQklFRThaR2tNOVRRbFdWekpmVUM1OE1iNW9oa293cTdPam1zcUZZWVJYQVdVY3Z3elFvaXUyWjIxUTh5bnJoM3VNRHZhWXlvQjV1ODFvZFozejFjOTQyR2dRUzBtTW9iVjZpY3MzcnZQU3ZYdGN2M0dEN2N2YnJLN2FVSkJ5dUd1MlBTa2l6NXVkSnZWV2phMHJXM3oxM1hjUUtPSlpUSy9YNWZCZ242UERJNlJ5L0lkOE9TSzhQRDd5YllpcENvMXlMNFF1YkwvRmRLRnd0YnBEVUM1VEQrS0pnRHdndlRaZUJKMFFIbWtLN3lDbGtJZFhKdVdsOThpMzkxYTNQWlJuQjZhc0E4NmwyMEppdVE0U0F1R0xLcFkwR2RtUUtCT2VHRkh1WmhhR0cwYVVCb3JDdStHTkZ5YVMyM1pMMWs3UFYxMUpZUzU3OHdzTnRxbm9Ta3B5WHJNNExNSHpFbVRoa2RLWForSlBnYjErMjEzTnZnaEdhNGYyTW9ZZ3JQSDJPMitYTE5LcFM3akpFbzZNdGhaZ0lTUXlVRWdwU1hUQ1lEU2szKzF4Zkh6SzBjRWhvMzZQMlhoTU9rL1Fzem56MFpDNFB5UWVER0U2NFZJWTBIVFc1QUNJcEtRWkJ0VHJVWjZSN0xQZmJjV2xjNjlHaVplWFc1UU5DNkVZbVdYV2J5TThqSlpyODFsclI3eDU2ekxqNlp6aFpNcDRPbWMrT0NNWjlwaS9rUFNSSEV6bVJLdXJKR3RyVEo0OTQvbFBmd1pSaUtwSDFEcXJiRisvenAyWFh1YmE5ZXRzYm0yeHVyYml3ajlUdS9oekQyNHg3TFBaaVBWR2pWcGptNjNMVzdrQlNRdnRyVWY5enlORmlLQndDd2l4R0hSZHNxcnJpc1BSNUxMbDBrSHBaa0s2c2pzcmdLR1ZnUjJMaytwY3ZiZ29PUFRtWjJVVnBLK0N6WWZmWWpGOXlCOXQ1d2xLZ2tJSnVMaFhFSVZEVEhnYWZMUEVIVlVkVUdUVWtWek9xeXNyRUxOMFBsOE1HQmZUVXNxaXA2S0ZNTUlzK1hmS3c2MmlOUE1IaDZaZy9YbTVnYm1FV1JRRWxzeUVZd2N5TWsvQzBjWTNyN2hkdUJhZTFkVHErVExOUHNhZ1UyMkhYV2RuN0Ivc2MvamlrSGd5aGlTRzFKREdLZkY0eEt6WFJRekhCSEdNVEJJaURBMERpYllCSDF1aHBDMFZrVklFeWlVQlo4eTZCVk4yMGErS1BDbzhPL0t5WVpmMnVqeXpzRWYxSDBSL3hxUGNMQ0kwS1pkYklicFRCMWJSQmlaSnltU1dNcGpNT0J4TzZJMUhyRXBCY3p3azJYOU9vaVF6cVpqVkcweFgxeGsvZnNLenYva2JaQlNod3dCVnE5dEQ0ZVc3M0x4MWc4dVhyN0N5c29JS0E0UU1jak5UZHExWnoxajIwbXYzdVJ2UXVwZ1RtV0tDTDV6WDN5emsvSGxiSUNGS3VuMmh5OCtxOElSQUl2TmJpa1dIWTJteDVSR3h5LzZKeGQyZThkYU41YmJjbEpTaGhlSlJMODRJQkpRVENNclpIVUZwN2xheUx2c3ZpM2ZpNVh0U0h3ZnRQVE5HbEVJOWpLaklraW9QMkhKZCtxTDB1RlROWUNxQ0pGK29WSUVuR2xHT2NmWU1Gbm4wbFRCZTBXTWdsZDdnU3hRbmVRNEs4Y1E3K2M3WnRnMVp1R2VhcG95R0k4NjY1eHp1NzNQd1lwL3pveE9TNlF5WmF0SmtEdE01eVdDQUdBMXBHbzBDUXBPeUFrUUlsSUFnVW9SU0VRWWg1OU1KMDFuTVdsZ2o4b0NtQnBPWGo3a0ZPcHVYNTdKblVlNXg4MGUyOEoxTDRUditaTzQvOEZGclZUNmhoYnZhbDAzck5LLzZHbExRYUNqVzYwM1cyazFPdWtOZXU3cEpRMmxtOHhtVCtaejVmTTVzT2lPZURVaVBKZE1VdWtLd3J3VXJXNWNaUDl2bHhZY2ZJbXMxVWlscHJxMXkrK1Y3M0hycEx0ZHVYR2RyYTV0YXZZbFUvc0l6TFVKb1V4Y3RsLzI4VXJqWmd5aGRCT1dqc2hCVmFlT3QxRXlWTFZteDJsWjdhUzdlNHZuaW40S0lWSDR2aEJHVVVzdEYrV1VYSHB6VlVLWkZGU0srREpRc2xrcWVyUkNvbEc1WitjWnpBWVlwVzVoTE8wcmhpVStLLy85RmtnWHJGRFFsQjJOMUVaUC9PTm83bVB3NW9LbTBMUDZTVTNvNnNFbzVrUkZ5aFJlZGh4Y1FXYXA4d0FPQmtzOE1CRUZ1bEpwTzV3eUhBM3E5QWIxdWwrT2pRN3JIcCtoWmpOS2FlRG9sR1k2Snozc3duaERwbE1Cb2xERUVPaUdTaHUxR25acVVLQlVST0k2QWNQRmQyV2N4UWpBejVLNi9jaHBPOWorbXBIWlV3dUZJVEFIZUxIcGVRekhPMEc1S1hsQ2dqV1FwU2dzUEwyYTBpeHdYcHV6QXlnYXkyaUJTUTJCUzFtdVNyVVlBTWdLcDBBYm1pV2FheEV4bWM2YmptT05ad3ZUNGhJMDRobjFEREV5QXBObEViMnp3Y0dlSEo4MGZRaGhBR05IZTJHVGo4aFd1WHIvT3RSdFgyZHJlb3RWdUVSV1RQUS9jVXJaLzU2eEhiU3FYRUNVN2RXYm15bEhnZVloTVZTMXFsdWJ3VmJUcXBVcTVHcjJlRC9aRTVSMnFHTytNcUt5L2w2Z1BEWXZGYnpIbzk2R2d4c3NNWHhpNGtSTml5aitrUDB4Yi9wcWJrcjY0ckUyMEpiVzdweXRaQjlyUENmRHRrQ1ZmaFNkdnJWWU9sWFdNVDJZcHVkWk1pVUJBS2YxZCtLWWo2VElDTTVxc1JKaVVoeDkreElObnp6ay9PeU9aelpGYVE1SWdaek5tdlI3QmFFVERDQlNHQnFDTUlSS1NVRnBzdUZJQjR6UmxNcCt5MW1nU2FlMkJDTnhJU2hTeTJFQktaeGt1QkZLNXcwOTRuMTAraXhFNVRCUVBjbUxYaTA0cG1JZUNTcElrb1FBUUZ0ZFAvdEpMdi96TkRGZmF5WmlMeURUL2REYXU5SllZbFBSazNkcHVENEpBMEF4Q1RDMkFGY0ZtSWprKzdmTEcxUTEwUEdVNG1US1p4VXluZmRnZm9RK2VNeE9Da1lGdTFPQmtiWjJUdFRYdTErc2tHSXhTckc5dmMvdm1MUWFQSG5CTjJNOWFaakFNVHo3dDI2TnpiNzRvNWliWkdoVk5HZHBweXN4c1V5cEhxNW9UajhVcmlyQ1hJckhabjZlSkhOWlp2ajVOaWU1UWlqK3Rlai84aTlDclQwcXpDZTl0Q1RLeXFYVmtlZjIzL3lBS0Y0dWcwd0lGbFQ5bnVuRFlDVlVncm9RalFPYkNEVTlGNXc4dkJGNEdueTYrUldtOFhEbTNSVkFGYWRoUTJENExsYURmam1RalZsTWFwSWhLSXE3dk9ETnVaNmExSHdWZEVGUXk1Nm1VZ2tnYnByLzRpTDVRQ0JWaUpqTk1taEFCVFdsb0pnbnRNR0Exc2k5Nm9CUUtVRzU0NkRwVTVxa2JRS2E2aUxNVTNzTW5DdDZQa3NLdCtVeHBkWnZ2K04yYVNidGNCOStNbm1uenRmdTlwbkhDY0RqbS9PeU1vNk1Ebmp4OFNFU0t4SERycFR0Y3ZuYWRyZTB0VmpzdEFGS2wwSVJXTjJDMCt6M0lETW5uckJxdXI5YTZPRXJkM0NpUVZ2T1BNQ1F1VVVtYUlxakZHTWM3VEJXUk1HeldKSTFtRGJuZXdFaEJxaFJKa2pLYkp3eW5NOGF6bEwzQmtPSEJpR2IvbE40c1lhb05zMW9OOCt3cCttYy80VzZnQ1d2S0d0S0U4VVF6dGtTU21YZmJMRkdrQ1ZQQ29TdHZlNUJqMFV3bCt0dFFSblpuYTExanlvSXVzaTJDeUE5SlFYWGQ3c00rZllWcjJlNXRURGtMd2xTYUZFRjVDSi96WDF5QkZQaHJBOTkzbkJGV2ZHdXZsREluamhvaEN5eVZLRUkxeUJKa2hQREtlN2QxbFY2N0lmTk1WU2QyV2ZRMjVWTk5WMGxvWi92TUJuUEtXVmUxTnhiTkNVYkNJTFdzbEZiQ2U2bEZFUWhDeGNVbGRMN3F5ei9ZVEc4djdJc1lwb2JidFRxOXlZejViSXcybWpBS3FBY2hqVkRSSGZSWWE5WllDUUp2cGVSTHA2MUJSV2FyVHlGSWRWWmVheGZ3SUhMN3NSVFdqS3VjZUtNSWp2UU1TTzVoMDBJd0Y0S3BrSXhWU0tvQ1pnSW1RbkF1QkovKzhJZjg2SU1QdUhMMUtsY3VYMlo3YzVQMVRvZnhvTXR2L0lQZll2L3dpRi84NGlPZS9wdEhqSHA5Z3VHQWFkRGtrZ3hwa2RJd210Z0k1b0ZpSGdha0luWkNHbnVRWlRvS2U1d0h5RUFnWmJHYWROdFUrLzJuM3QxbWl0V2xFTWEyUVZJZ2xDU1VCbEVQV0trSGJLMjFiYWUvZDhaZ091TnlwODRvbk5HZnpobk5KMXlLNFZxanhVcWtDS1QxRE9SeDVWNmNmVmtvSjhHa1huVmJET0NrUzk3SkIzNVpGU01LWXBEMEsxbWZ1WkNSc1kxVGNub3Z2NkNzd1pHZVpiaGtWUExublNKci96TEFkclY5RnhVVEYyNG82dWNqRno5ODRHZUtDd0ZheWp6VkpFZHpTVzhZbUxQRmMvWnUzamFZQ3ViSkFrdGxFYldVdjF5VUNiL2E1T2pHYklpWTU2OFlYOGlRNW5iUy9KZFRWVW9KYVlVcWxQc3huN2FpdlQxc3ZoR283SE5MRm1CbmdaVVpoc05ZOVprUUVEVnFlVitsODViRkJYeGs5RVF2ZkJTZEJVaEs1L2FqRUI2SnduK2ZyVGwxSm9iS0FTMFFPaEpzYkxma2FLR0lnVmdLNWtJeXhYQnNETE5XazE2cnhjcjJOdHUzYm5QMzhqYnRGUnM3M21pM0NKUkVweWxwcWprNVBzU1ltTHV2M3VPbDErNWhrR2l0bVU5akJ0MGhaeWNuSE93K1orL3hRODUybmhBUGVnenZDQjVFRFRaRlFnZE5tTVNFSWtXbENVRm1YMVpXR3F6Y1RFTTRINGJFb0ZOdnJHYTgzRU1aMk1HZXNyOERqUzZ5NzR6TE9UUUNkRXBkaFZ4cDF4R3QwTTRVMHBSUUtwUUVLWFFSOHBuZHpMcFFuV2FiQXVPSHBmbzRNdUVGeGhyZm01TGxUV1EraS9KV0lJT2Mrc05CSTNUK0VHWUhkNUJCUDBvOERsOStYeFlFRkJxWExCdXVraTJtUGNpcjhSRnRMbXpHNlFCelhKNDIrZmZncDc5amhDcVk5VklzdUE3OGZicjJUNjVzVTVvUEZqVUwxSVdTR1VrdW1aQzZGOFpOL216bUdibnN0SkR4ZWo1MHNrckVPKzJkMVZSV0JqSzVJQ2tyckdVRkk0VE9WWUlsVFZ6RnppaUUxYUhiaXRpZ0pYbTZzc2ErbkxHRGNjcDhrQ2Nxa2dhcnBjKzJLcElpcmx4blVtZGgrMmNqd0NoRklpU3pJQ1ExbXFFeERJeG1iRFF6SVZIcmwxaTdlbzBydDIvdzF0WVdhOXZicks2dDBXZzBTRk5ORXM5ZGkrYk9YYTF6NGt6eDhFcWtzcDRHS1JXcVU2UGViTEY5NHlwdnZQZXVOU1FsS2NQQmdMT1RZdzVmdk9CZ1o0ZUhqeDdRMzl0QjludXNpSlNWUU5BUmhrNG9NU0lrRFVQU3dJRkQ4bndJU2tUbnJNSUtwVFU0SVZMM3Zka0ZKNDdncTFOQm1xYXVsY3BpRUJLa01TZ3BrRExGS09VNmxDTERJQ3ZYTXdsN2RpTm5oM1Z1Y3M1TVhWS1VuSStsalpPL2t4S0xucFRGTmJmTU16Q0V6NUxLU0ZIRzIrcklDbnZPMXdZWVU3SEZlMnhJdEhzWlpZNHY5MHNSbjRJbGhNRW9Cd1hOU281UzVwdkk5c3Zrd3lpZHU1NDhaSjR6TVZCYXJZaXlxQWQvMk9jRmZIb2JnNnowazc2OVdoUjhOMCtFVlhnSmhNRElERDdoYlE2eWVLMWxqa2poZTk2Tkp6Mm1tQkNickhlekZhb1dXV3B2a21PK01KYW1JNEJFWmtNak8rc3d4dGdTV1VPYWFCVFdDaXlkajg1NHA0b1MwdDFXdUJRWVNTSWcxaGFmbFdnTmdXUnVKS2VrOU9zUnB6cWhjZVVxRzlldmNmUGFOVGEydGxoYlc2WFphaFBVYW1oU1o5KzFqL3RzT3NsZnV1d20xS1pRdkdsL09tck1RZ0NIVk81M1RHcm5NUXBXVnR0MDFqcmN1dmN5Z2w5SGFNMW8yT2Y4OUl5akZ5ODRmUDZjZzUxbmZQYjhHY25aTWVQdEd6d2dZQk5EbUtiVUREU01JVFRDRGdlemVZY0tpSlJBQmRJZHBkcEZhRXRQZ1ptaVUvS2YwZTl6cFJBZzFRTE5KMTk3bXNUT1NYU1JBVWpWeFpvbERPbHNqV1p5Z1pIeDBwcHlkNnlwUk1PNWQ2VDZsOUxDRzhhYXNnYkFNN3ZwUEJtcCt2MWxGa3ZIZnl4UlQweCtrQXRQVnEra3NNNVQ2ZHlWN3NHZWo4Y01lK2NaRW16UlhTdkVvb0JCNUllQXYyNDBsVFdSS08xVEY1MTVvc0p0OVcyc0Jaa251OW1sS1U3WlRFSmJiUG5Oa2hxaUxOaklEcDZ5MEZHNFhQVml5Rm1FaEhyaUJ2ZnphazlIWHZKekNkZmJaaFdwTWFWWlNoWm1LZk9Cb3R1MHVCdE5DR2tGS1VGQUhBVE1wV0tTYXFZWUprSXcwcHEwMldiOXppMjJiOTdpYTlldXNMNXhpVWFuVFJoR2hGS1F1bUFSNlI3NHhDUWwreWVwSmhHSlJaR0pyTjJSM25KWkY2MkttN1JZelh0UWR0RzVLc1pxekVXK29zU2JYRGZiVFJydEZ0ZHYzd1MraFRHUytYekdlRGppNU9pSW8rZlBPWHkydy9HVGg0eWVQcUlSejFpVmdyYlF0SldoRlFaMmRxQUNFcUVJaFB2c3NnQlZwN3JNV3pNQlF1czhwZGlJSXRMYmVBZTg4UWQ0V2x1T2dsYzk1cnlKMGdCTzQ1V2VudDdGTVFFeS9ZWDM4bVR6UkdtS0ZPbGxUc0NzTnMyVG43MERXTHNiTjQrZXoydGw0ODI1S3RMbTdNL1gybTQ3dGNXOUsyQSttVEFjOUJpY25YQitlTUN6eHc5NS91UXhUKzkvU25kL2w4RDIwTExreEJNc0NmQVEzb29wVTVWNWkwdVQ5VG1tWXI2dDVMK0pYRGNndlpQWk9aWDhzdDRUUXhqUGpGT3huWmVXTGJJU0JHSThZWjkvUVBsR2NlRmxzRnNtdHltODdrWVVjVlBHU2t4OGNJbko1Z2w1ZXF6OUhOSzhYUkgydjVIQ1JUUVhLUEpZQk15QTh4RDZZY2l4RU9oMmk4MDd0N2w4NHdicm01dGMycnhFWjIwRkZZWXVNZGgrdnhrb1ZPc0VVaDhmcm5OcWprRGtIUDVVYXc5Mm11WUJMVUxhMjNJNm1kTHQ5VGsvNjNKK2NzN2F4aVl5bEdWQ2tIREpzNjRYMWo0NHcvM2VNMEJuN3JOQVVtdlVxVFVhckc5djhNcmJiNEF4SkxNSjArR1E0Nk1UOW5aMk9YcXh5eWVQSGpGODlvUm1QR2V3ZG9uVkZOWlVTRk1vSXFGUjhSeVphbHRGaVNJSjNMWjB5cTRrczBUZm5OU1VIVkEyMTBzbkNTaVQrd01XWXNOTmtlMXdnY3kxMkNSNFRiOC94OEFzcHdHWkNyeWtGQnhUMGY5bkw3NFNEcWttUUVvLzg4L2tMWHFXdnB3bU1hTmVsMkgzak1QOUYrenRQT0hwd3dmc1B2eU1rK2M3bU5rRUpiRWNqRFJGR3MxcXAwa2d0SEU5cit0ZlBSU3k4TGVCQlpxZlludFNaSkZKUXprcjNsY29aYWtsTWdOY3VzZERaOEdXeGF6QjRLa0hzMGwzN3BNdUJpRlppR00xbE5UNFV1QlM5SklwUkJ4NXlTU0w0WTcwS2ZJeWR6cG1yWURHNnZoVFk5ZUYwa0V4ZkthaXhoSnJOSkpFQ0daU01GQ0tXTUxVd0V4SllnUkpGS0UyTjdqeTBzdThjdTBLblV0cnJEa09nRkNDVkNldVJYYUk4RFNCTlBHWXlwbjZUK2ZiVXA5bVkwdkRGSkxVZWN3VmdWQUlZRGljME8zMU9UMDk1K2pna0NkUEhuTjRjTUI0TkNRQWZ2RDlIM0w1MmpYZWVlOWQzbnp6RFc3ZXVzSG01Z1pibTF2VTZ6V2tVcVNaaDhQb0VoWFhtQ3hHM09SOWFKWUg2VHYwZ2xwRXA3WkJlK01TTDczeGVsNlN6NmN6dXFkZGp2WVBPTjU3em90bk94eDk5akdtZTh4S0tsbkIyR3BCQ2lKcGlGVnNuNUF3Y0lwSFFXYW9sVzQ3bFIzZ2FWNkZ5WHdiSkx6QmtCVXpVYVJBWmVFc1hvU1hFcUpzTWM1NmVDbzVCeFRjL2V5Wkt5dDBzME5BZTNaN1Uwa1FnbFNuYUxlWmtFSVR1ZzNSYkRoZzFEM2w5UENJL2QyblBIbndHVTgrKzVUencxMlM4Y0RPUW9SOTJaV0FqVnJJOXRWTnJsKzl6T1pxaDJhelJxZFpweDRGQkw0Q1J4aVRyK1FLNjNlaEdDdWZlTklSZjRvQXhGenBaSFNaTXBUMTVWcDRRUlhGVkRLRE1raFRFVkRrYWl2aFdXOExHWkQwL0pEWndTRnk2YWJURkFodlNPSmhvWEs2a0N1ZFpaNzk1Z1F2U2hicE50cGlRZUpVRTJlYksvY1NwTml0UXlJa00yQmlZSkJxenFPUVl3SDFWcHVWSzVlNWZPY21HMWV2c0xXOXlhV05UVnJ0Rll5MFh6T081emxMM3hpTk50SnFIa3pab3BudGJ6TjdzOHhxRHN1eHNCY0FBQ0FBU1VSQlZPRlZWKzZoaitjSjNWNlBzMjZYazZNVERsOGM4ZUQrQTg1T2owbTF0aFNoTEpYV1lCTnJsV0k4bmZIdy9uMCsrY1ZIVGhnRVNpazZxNnU4OS9Xdjg3V3ZmNDNiZCs1dzVlcGx0aTl2czdLeVFsQ0wzTVpITzdhZHlsSG0xbTVmcEVxN3NXaUJzTTVXck5MUWFLL1FiRi9pNnUyWDNUT1hrc3duZE05T09EOCs0V2h2bjJlUEh2RFovYytZN3UraHc1VDFwbVEzYU5BeUlRMk1WVmlTV3BlbUJwUG9ITGhLbXVZU2NPVmtrVUw0dVk3dSs1RVZkSTd3MFZFVXljR20zSnNYUWtqdEJYVzQ1MWlZZ2hya1VhOTlWeVVpSmN4MHFtbkNiRGpnL1BTVXM2TkREcDd2OFB6eFl6NzU4R2NjNys3QWZHTEZhUktFU1ltVW9ObXFjMm03elpYdFRiWTMxdGhZN2RCcDFtZzE2b1JLMnVvdlRiMzVRMEpRN1ZheTVDdDg2RUVKWjZ6eFRXVmlpY0RZdHg0VUFmRUZka3NzOEhwU3IzUTBSWWV2UmNsNGtSRnBoYW1zUHJMOWcxbW11UFpQK2VKQWs5bkl6N0g2TWpCRUJ1R1FVbWFWYlQ2NWpvMWdiQXk5MUI0Q3NUUWtRakNZeDR3RFJXMXJtODBidDdoNyt4WmYyMWludGJaS3M5RWlxa1VJaVJYQU9ESFJaRHBhNE5GbHlra3BQZDJrdGdteE9xTWJDRGVkbHhZeU9vOW5qRWNUenMvT09ENCs1dkRnaUwzZDUrenU3RENkemV3YVRLcDhjMlJrRFNOaHJtMmxrdld0cEFrQmhtWnJCV1VNVXFmVWpMSHpEWk9TbUpTZi9QakgvUFVQZjBBYXg1WjRoT0hsMTkva3ZXKzh6OHQzNzNMcjlpMnVYYi9PeHVZbWpXYkRXbnlOTHRBeCtVUy82cnZMZnI5cGllbVBNQVMxa0V0WHQ5bTRzczI5dDkvazIrYTc2RFJsMUI5d2RuTEc4Y0VSeng4L1l1ZnBZeVo3endoSFBWYWtvRzRNSFNRTkE0RlV6SVVpMWJhQ1EyS1pqQ3BBcExHN2dHUytHcGJHZTliOXBNN2NzN3VZLzFEU3B4c0hoM0hQdG0wNU5GSlo3WXFRZ2xBS0lnRnBNbU0rbVhCMmZNRDV5UkdIdTdzOGYvcVlqei80S2Z0UDdrTThkOXZKRkowbUJNS3cwZTZ3ZVdXTDdjMDF0amN1Y1dtbFNhZFpvMVlMcVlVS1lWSW43WFlxR1QwbE1VVTZ0OWRJMjJRZ0lZcnNlRk1sblM2QkdlV2dCYkdjUkdxRVdBSTJyTGdhU2ptQjFyWGxHNUtFZDNMaThjN0srOFNxWk5KM0VjaUsvTmg0RGtLN2l4ZlN5bUl6ZEpXUnhpS3RWRUNnQWtJbG1LdUFuZ3FZR1RnTlFnWTZwUmNHckw5MGo2dTM3bkRseGpYV0xxM1JXVjBoQ2tOa1VFTWJUWnJHZG45dEJLa2JyR1ZRVUZPRkxoaDh2YTZ6RzJnUE9TYlJTY0o0TXFQYlBlZjB0TXZCL2hHSGh3ZnNQbmxDcjN1ZXcwV3pOV1NpSVRFQjJralNGS1FKOGlySmVEdmxmSkl0SkZwbzBvejlMMTBldHNwbWx6by9vQU90TWFuRzZJU2RuVjJlUG5xRXd0aWZXV3VhblhXKytvMXY4UExMZDdsMzcyVnUzYm5CbFN1WFdWbGRvOTZvNStJWS82SXBabWxwb1prdzNnb3ZIeXNJbEFycGJHelF1YlRKN1ZkZjU1dmYvZnVRYXNiaklZUHpjMDRPOXpsNnZzUGVvNGVjUFBpTTlPU0UvcVZ0SGhDd3JTS0c5U2JKZUpRUERQTlEwcHhOa1Jia3BUSlVFS1QwQWs5eTA3Qm4xblZEWmVNR2NXNWRuTXpuVFB0RCt1Zm5IQi91YzdEemxLUG5Peng5ZUorSEgzK0FTT1kwSTh1SHRHejRoSzFXZ3l1M3JyTzl1Y2JHK2lxWFZsdTBHM1dpTUVEWjZaSlRYV2JPRDN2REd4KzFuNzBIdWNmRkp5WkpBcHh5VG9qUGczTXVVL3Q3K0FhUEJsVEcrbGZwSXFJOGFERlVjY0tsT3NLM1Q1WmhRYm9NUVZpb1BUekN1ejNTYzRhZDNiWHJYTEtzdFhid1NjRnNOcWZmN2JQejZDbUgrNGZzVFNiTU45WUpybDNuOG8yYjNMdTh6ZmJsYmRxZEZrRVVJWU13cjBxeStVT2lFL3NZU09sYUZGUFloa1dHV0ZOT3ArOW5Ja2kzK3paTVoxT0d3d0hkOHo3SEp5YzhmZktFRjN2N0hPMGZNSi9OWFVsdFA0OUVRMHFBU1lUendXVWNQcGxIWW9HeG5BQWg4dkFNbWNtL002T015R2pGRU1lWm5Gdm11NXJVZzJ3S0Via0ZjZ1NSUmhwajkvS3AzY1dQNDVTLy9JdS80UHZmKzU3VjBTY3pvbnJFdmRmZjR0WFhYK0hOdDk3azlwM2JYTDk1azgzTkRack5WbEVxWjNiZTBzRXR5djhjNDFTcDJWUXF3U2hCbzlPaTBXNXgrZVoxM25yL1BiU2VFMCtuOU0rNkhCMGVzYmZ6bkJkUG4zTDQ4QUdqMWpGUDkzWWhrRHgvOGdRcG9OTnBJa083Zmt5U2hNUVloTEpxTEptMXZWTGtMWXlWajJ2SElOUW9ZWkJHRTArR3pBWUR6bzRPZWZIOEtZL3ZmOGJPL2Z2c1B2eVV3ZW1SYXk5dG0ycE1RaU5RM0xwK2xadFh0N20wMXVMUzZpcXJyVHExZWtBVUJraVRZa3ppRHNRa0d5MlhMaENmWWloOEYwRCtHZmxKUllYdFYvelpzK0hua01zdllneVh5ZkVWVWxtRlRpSVFGVXhxTGpWZUFJOHZIakFtejdDcmJpb05ZcWtOU2VRWkFBV20zcjJrU1V5SUpwM1BHSFpQT1g3eUdYLzRmLzRmdlA3NnF3d0hRK1p4d3RiV05sZXVYR0YxZlozV1dvZFdvMmFaZlViazJYT0ZjMUI2ZTkyS25takpRTWp1bmxPa2xQa3RQNXRNR2ZRSG5KMzEyWC94Z3NkUEhuTjhkTVJvTUhDZmtjcFhrYW0ybTRoWTI0Y3hKWXRldzdNOU8rVmc3dmN2RGt3cDdBT3Nnc0RCUkFWU1daRG1mREpFbW9RMzNub2RuWUxXcVgwSmtwUTRUb25qT1lsakllVG0yMUxnalZmMW1Xdy80cllqSnNYb0JKUEV0Zy9Ib0hXQ1ZJcnRhOWQ1NWQ0cnZQZnVlMXk1Y1owYk4yK3dzYm5PeW1xSFVNa2lUMDhLUjVUeTA1TkZlWWZ2MDZaTVdyU1NSaFFKeVVhU0pMYUZPRGs1NXVSb24vM2RYUjQ5dUU4Y3o3bHgreGJ0Wm9PZi9md1hmT2UvK3hlMHRtOWdaR2dWQ2JyQXdDa004K21FZnZlTS90a1pSeStlcy8vc0tVL3VmOHJCemhPR0o0ZElIUk1HZG5zQ0dpVUZuWGFMeTVlMzJGeGZZYVhUWVRhYkVNbVV1emV2SVVYcTJsTlpjQnJ5cHFtRTlTZ2xjT01EYWl1S3U1eG5ZVXlPRWl1OUxYKzJPelNsVUVhekRGM3N2NkRHMjRuNmtscktVU2VsUDBndStYdExUb3hLeUdHaHJQcWN5c1NJTDdBZVc4bm5ySGZLRC83NC8rWEZvd2VjdkhoR1lCS3VYdG5pMnRWclhMMTZtYlZMbDZqWG02aEFvZHlEbGhnTHpVd1Rxem9UWk56QVlxR1lEWkdrVW1WU3NiR3FMT0U4dGRQcGlGNS9RUGZzbktQakk1NDgzZUhKb3gwRzNYUFNKQUZwQjNKWkNhK050RzQ5R2JxMlV6cDl2ZkgyeDZLMG1zbWxzaTZSSjFBS3BXeGxJYVVxNHJXOHlaTjJFL3JKZUloU0NXKysvbHFaOUtTTHoxVWJReEpuQjBQQ1BFbUlrOVNKaW1ScGFKYnhFcVFEYjloTlRxYnExT2cwSnA3UEVXbUtNSW1WSk1jeEdFMnQxZWJxdFd1OC84MXY4TW9yOTdqejBpMnVYcnRxL2YrTk9pcUlpdEkySHhJVlNVMjVZTWlUN21hRFBHTmtRWVBPQnIra3BQR0VmditNMDZNVERsNjg0UG51YzU3dVBHZVdDdGF1M21UaitpMGE3VlVPRHc1NDhld1pEejcraUVlZi9vTEI2VEhKZE95NGk0WkFHbXBSeUdxbnhiWHREYTVlMldKemZZM1ZUb3QydlVZWUtmdDlKZ2tIUjZla3lad2JWemVkRmtWNm9Uakd3KzBYejE0bXc5ZGVFbGVKeTJGRUpWL0lhNk9rcUt6cEJVR2VaUzYwNTZPdjRvZzhyendGSFZhWUplUU9EeTVZS0lJTFhieklqVCs2a3RZaEtneCtYUTVROVAzWkZ4cXVxMFdMeWZ2SndkRSszL3ZELzR2Zit5Zi9tUFZmL3hyMWVzMXg2Tnh0S1l2VlMycUtZc3JQYXk4WUFsYWZMbHkvSjZXeVA0S1U2RlF6bmM3cGQ3dWNuSjZ3dDNmQTR3Y1BlTGJ6aFBrOGRidHErNmdtR2pBS0xSV3B5dyt3QjRIL0VvbHlIMnd5NTZWN0JZeTkwYVcwVVdIV1FLTnNrZzNGRFpCSlhzM2kxTkd0bVlvb2FiUXVKU1RuMEZBTVVhU0lRZ2tpeXZrRFdodmlKTFdIUTVveW55ZWsyaG1VVEVHcFNUSlJyRkNJUUJLR05WUTI5YzdnSTJtQ1RsTDI5dmJaKzhOL2pVNFRralJCQ3NuRzFpWnZ2L3N1WDMzM1BWNTYrUjVYcjEzbnl0WEwxQnNOYXJVSWhDSFZxYmQrVEl0b3M0eUdMSW9EMnFRcFFnWWdGRVlFQkxVMmpVNUthMjNPeWppaGVUTGdyLzdrVDNqMjdQOTJ1WHJTcnRlMExjZkRVSEpyczgzRytuVXViMXhpWTdWRnAxV2pXWStvUnlHUnRGVldialFYcVIzbUlYaXhmNEJTaWh0WHQyMjFVb0FBUE5BdVhuQ3RBUkY0VjJvNW5VbUlLbC9mY3hsbUEzQk5IdGFSdlZHQkVMbWZyaENxR0QrYVdKZGViTEdvaXlpbDhGYURPZXpsbFhvR1JWMStnVDJQZEw1Mk5SWElnSjhWV0NVTCtWTmtqM2RtL1NKKzNMZ2dDa011WDc1TVdLKzVveUZYaFdPY1cwdUt3bHRRSnY1UU1rMlpKR1U2bTlIdjlUZzc2M0owZU15TGcwTWVQYnpQNmZFSmdReVFRWkNENDFJRGliWTgrOVNSZGZOZm8vQkthbE5KNm5HQ2pBQ0Jrc3F5QkpWQ1pac0s0VTAvS3RRSGthTzdqU2Z2cmpBU2pTZjk5bHhvd2dPTkZKZzFVZHJxNUZ3aFphc05hcWFRd1RxRVdaSnFramhtbnN4SVlvTk9VODlxN1pKK01veWFrS0FVUWhtQ3FPYUdYSVpJMjZuMmFETGxSOS8vQVgvMUgvK0NKSmtUeitmSU1PRG0zVmQ0NzJ2dmMrdjJEZTYrZkljYk42Nnh2YjFKdTkwaUNLUGM3eUdFUkdzWWo4YjArMzMyOW5ZNVBqN2p3ZjFIUEhuOGlCOTgvL3M4L3VRVEdxMEdVUlRrektSbVBlTGFsUzJ1Ym0yd3Ribk9aRFJpWTMyVjIzZHVFS2tBU1lwSkU2dlgwSzdkMUNsWmdHRldxZG01cnVMcHpoN3RacDNMVzVlc21NdWZpa3RSZHZaNVE3WFNIV21xK010S0crb0pqb1FmMEdGa3JvWUY0N3dBbnRmUUxGblNpR3JBbUNqRUhhWGlPK1BGQzFHZWJGY1dmNlZBVU1yQ29WeDlWazFhRVV0dXJ1b3N3cGdGekpncFNUUkV2dnJ6cVk3WjF3OEM1ZnozSWk5ZnBSVE01d25qMFpqejdobUhSMGZzN2o1ajU4a3o5cDQvWnpxWnVPR2RRZ3M3ZmRkaGk5U1Y4Y2IzVmdSRm01UzZ3MUpxUDZMTTlwZGhFQ0NWUkNuclpSY09TZTFUaWt4SlY4YXlwRWtuRDc0b0hhbHNQeE1sczlMeU5BdFRrV3lYYU5EZXpTcWM3VFlRaGpBTU1IV0ZvT1lzc1lJMFNVaTFKbzVqWnZPWXhMVVJwZmxDVnBrcEF6SndQM3M5cjFRQ05IV2RRcW81UGp6Z2ovL04vMk5iS2EzUlNVeG5iWjIzdnZwVjd0Mjd4OXRmZVFlRTVQSGpKengrL0lSUFAvcVlSNTkraUZDS1doQmdVbzNXQ1NwUWZPV051Mnh2YnJDOXNjNzZhcE5PSTZMZGJ0SnFSQ2dWOFBUWmM5cEJrMWRldW9yUmMwVGlERllhVzNFWVR4NnNyYnJWemxodG5QdWpKMDlaWDcvRTF2b3FtQmlmZ2lrcVNzRHF1MmVNYjN2WFZXaDdQZ3NvWFh5bFFzOGdIWGs2by84RWk2VzdQOW5KVUZNK0dVYm5mMDhJbng3c2tVYTBIOG50amV6eXZxYTYvOWI1ZFdROFNFSUpibmh4bmU5NlZWVmdxVHdZcVAyKzdKb25OU21KVGhHcExqeWVqaDJBa0tSeHpIQThzUy83NlJtblorYzhlZktFdzhORHpzL09TZE0wZHhtbVdwQm9ReXJyRHBZQ1dycmJQUHNaUlZHeUYvMlZYYnNwQklGVXFOQXlCSldMQ0JPZXFjcVVjRkJrVndoSTZmRlNUQ1VwclZCeG1vVnFhZG5PdHF3aUxDU3hvcnozTmtzZVJvb0FtTVhSamloY294NFhXd3FKREJXQlVkU2lrRTY3TVBVa2FZcE9OZkU4Sms0UzB0U0tlSEptaDVHNXAxOEloUllLR1lBS0lsU2tiYnN3SGFPRllqYWU4Tk1mL2hVLy9lRVArZGNPVEtOTmdqQ1dNL0RLcmF0YzJkNWk4OUlLYTUwMks2MDZqVVlkSmEzL1FPdlVWVDBXcnpJWmpmbnM0Uk5XVnpxODh0Sk5qSHZ4Yy9GYmlhZm9odzBaMHRTdVZaOCtmc0xsSzl1c3JiVHRxbEdiTXZaYlpLRTBQbmM5cXhKbENaWmIvSzRvQ2VSSzdXOEdTZldRWUtrRHQyZ05zM2xLa0FFd3lqd2pYZjZseXVXWnBtVU00YUlIMzFBaHJsNlFqUzRRWGhUNXNtdXFqQ2dUQzVYS2txR2krOFZJWnpFVUFoczJhYXg4Tms0U2hxTVJnMzZmazZOamRuZGY4UGp4STA1UFQ0bm5jd3NWa1lyRWxiTGFsZkdXdkdNSGZtbSs5NVVsSUl6VTNvbnVidTVRQlVnVklKUWt5TXAzNzNPVW52OGdyd2R5YVprc3pXS01McXpXeFFCM1NZMVZncnRlZkloYU5vSE1DdkpjRlZsQlNCUW9NanpjdTI4RTgrZzR4dmcxcXloV3pibkJ5NVF3VlFpSmltd3IwR28xeUZ5dkp0V2thVUthcE1SeFREeWRNNTNOaU9PWUpJbnRpNXBrUU5JMHB5TWJORkVnYWJlYlhON2U0dXIyQml1ZEJxMkdWY2JWUTRuTWtwRXlDSkFvakZRNjM5b0l1c01wSDMzOGdHdFhyL0xTN1N1MnZNOHRPcm9rT3pmZXk2L2RpanhPNE1Iamg5eTdjNU5PdSs0cWhTekh6eDM2enNGbVJKclB6N0w0TWg5bG4wMW1wVkRsZkFjTXFRN2N4V2NBTzkrSzA1alJaTXBrbWpBY1R1ajFoNXljblhOKzNtYzZtV1ZTNEVvcDRUVWJSVzU1MlJBdnFzTS9EN05sVTJsMDJjaHpnV2pJYjF5RVlVaytBYmx6U2hpUnkzNFh2K2VVc3FYUGhWNjRyRCtsN0FmeVozLzY1eHdmbmJDejg1VEpaRnI4SW8zZHJ0cEUzeEFqcEQwdC9lVGduRHZ2S0N2RyszbWR3aThReXRrdkxZdE91SmM5ajlVMlJac25URVZrNmVHVlRVYVM4WVk2WlQrbExMOTR4b09YTHQrNUxEa3NoYjlDS0dDWldBaU1NR1dzUTE3TDVUMjc4RFFnb2xKc0ZMOERXUUd5R2xNSmU4by9ET3ZKeUE0UENhVHhuTWxnd0hRMFlqNmRZTFFoVFJPTVRuT3dTRDJNV0ZscHNyMjV6dGIyQmhzckhkWmFkUnIxa0tnV1dmU1lTVW5TbVNzM1lsdjIrNUprSWF6UE9adVdDeENCNU9SMHhNOCsrb1JYWDc3TmpTc2JDSjJTQWRpRmttV2dycWhJbXd4TTV3a1BIdTl3Nys0ZEdvM1FWUTBPVklKWjNIQjVNVDg1Y3Q1TGtjcUt5dFN4RmgyTG10azhZVGFiMGg5TzZQYjduUGVHbko2ZDB4OE9tU2NwUVJCUmkycEVVVWlvQXRiVzFsRnJFT0NkekNWUmM3YUw5SWRFL2pIbisvMTlycjdPREJpeWRHZ1lZMHJFbER3RU5MOHBYQStjZFM1YWxHOFY0M09ScEhmZ1hMQWl6TkpnSExaczQ5cDFmdXUvK1NmOCtNLy9sQWNmL2NMMjZnNWpoY2doMmxhZjc0cWUxR01XV2dXcGNTczJnWlNoWGJFcFJhQUNadzJXaFEzWFZJSlF6U0tyclZ6VytjVFlLaDR0aTdpU0M5NnlIRDlWeFV2am85bkZ4YmQvamw4MDVkN1QrUE1ZVVlTSytLcE1VOWxKR3ovR1RTNmRJZmlwVDBXS3JSZmFrczNDdEtGM2VrTDNZQi9jN2Q1cU5saGRiN0crMW1IajBocHJxMjFXMjAwYXRZaEdFSkM5ajlKRHdtZEROc3V6bEVVK29WR1plOHc1T3AwSEpQditaY1RoU1plUFB2bVVkNy95SnBjNk5RUXBHR1d6R3dHZFpzQVNyekxLV0pWU01adkVQSHEweTZzdjM2RlpEKzN2SW5Xd0UrZkNsZDdRMjNoVGYrMmNqVW9vdExGbXBuaWVNSm5PNkUrbURNY3p1dDBCeDZlbm5KNmRNNXZPTFV1aFZpZXExYWdIQVdFVXNYbHBDeUVOb1JJMGFqV2F6U2FySzIxYXpScnRWZ1B4Wjg5NjVZZ2Y0UXR0WktGMGMzb0FlK3JvaGFtaXY4NFRub2locUE2TWw5MldRUStFeCtNd1hnNTdkaHU1bFVXTzVwSzU2cTU0TUUybG1QRCt2N2VtaUFLRlNPZTgrT1RuL08vLzR6OUdLb3ZBMUdpVUU1Wm9VUWgzSk1MQ09wUjBMM3BnNmJJeXk0MmpFcUhseVowcjhlVStIRk9MNmtqeVM2ZEFsdzRGa1VtSy9RanJUUGdqQkJkMy9xSWtYTTJVNGZQcEdDVVNYbjN0bFpJQlNaUnVJdWMxbDlKckFaem9Sc2lGMlk1L3ErVy9hNiswTkNXeGl2ZjMwNWlURjNza3d5R3YzYjNCNjNkdjBtN1hhZFJyQk1xYVg0emJET1F4M3ZuR1NWUkNPNlhYRDN1dVVZOTduODJKTFBCVklsVEk4NE5USGo3YTRXdnZ2VU83b1NCTnZMYW9zQTRMQ3VDb2pZcTNYM3M4alhuODVCbXYzSHVKV21ES2NWNDVkRWpsN2ErMWUwdG1xV1lTenhtTjV3d0dFODY3WGM1NlBZNVB1d3o3UTR1aHE5V0l3b2dnQ0FoRHEvVUlIQXdsakVJYXRUcXRWbzFPdTBHclhxZlZyQkZGQWFGTG94S2V0eUVRUzRBZ29rTGVFZGxOWmJKNEpPVmVjRm1oN0hrbHVoQ2x1WUFRWmE1NmRxMUtJY3Fxdmh4TWFqdzNsZkJRWGVWSUplRkJFZTJ1MTJQOVpjbEVXSHBLaXJYalN1Vzg4RHFqN2txQ3dQYm5Va21VVkM1cFZoUzllSFh0NmFvakthU2xBTG1Pc0lTUXluTWtEYVVScC9IaHBMLzh5KzhsZFhvMDJld0c5bzFYUzFaL2k3cEpqNEhvNTh1TGhUVXJGQWl4OHA4bkwwaTJxYWcxYzZGS01WUTBPZjdkUGsyejhZakQzV2ZJWk1hdnZQc0c3Ny96Q3NJa0NKT2k5ZHpPb0xXeElKQjhLSzBLRzdtdWVBY3EzaE9UUDAvRjRhZUY0d3dpU1ZFOGZiclAwZkVwMzM3L1hhSVFNSWszbnRZRkY4QUxIODNjcllpQTRYakdzOTA5M25qOUhvSEtVcU95Z2JvazBTbXh0Z0VwNC9HYzg4R1E4MjZmOCs2QWZuL0lkRFpGcUlCYXJVNmpWaU9LSXVyMUprWWJ0amJXQ0tRZ0NnTGE3UWFkZG90T3EwbXpIaExWUW1waFVOamRkZUV3dFFQZHhGR24zTXBYQ0FLUWl4bGtYbnFLOEtmTHdzT2U1Tk5KWFZocFBmSjRQc1QwUUdtaTFQSjc2VHUrZHQ4VC9tUkFSMnVzVU9YQlY0V0JLa1YxRHVqUlhaeVJRenVpak5hYVJqMmlIdGJzeTQ2bzlHOHVQTVFVVEg1anNwRmljV1BJekVmaGhEbCtCWU9mRkdlRVJ3VzZRRHhWMlhLS1VodGdQRFZsMlRCVk9yd3JmM3oyOVdTcGpTKzNUR1Zhc1cvUmNIcjd2UEpiVHNvUW56dmNYZFNIWlNtRlJVaW1lNFlNZEUvUE9EdDRRVTBKL3VGdi9ncXYzTndFUGJmcVNDTkswMnlrOHJaRlB2dlJWR0NkWmtuV01kNS9ZM0lDYzVKS1B2bjBFWW5XdlAvZVc0UXFkYk1lTHovUmdVL3lJWjJYSm15a3BEZWNjTllkOHZaYmJ6T1A1L1JHVTRiRENXZTlBZWY5QWVkblBmcURJZVBaQklNaUNrTnFVWlQzNXBmVzF4MmV3aEFvUWF2VllqQVlNMHRqL3Q2dmZvMlZUcDE2RkJGSWFTOUVveTFkT3NPTDZjUkw1L2JWcWw1bHFDM1ZleG83UzRmUWxZSFFSWkxhN01YT1UyV3MxTlVzR3k1ZE9Qd1RudWludkRJUXBWMno4T1lEc2hTWG5DRzY4MHJEZUdtc3dpdnZjbFJTbG5KamNsVmZFQVNvSU13WEhnVWhwdUN4eVN5Q3lkdkJtOUxkbWFYOXlodzhrYkVSL05LNWhKSDBrNWFvdnVUVkZZOVptTFliVTJLbmVkRmRSWWxwS2krbHZ1RDFORlZiZGg3ODZZdEgvQ0d0S0hNVXY4eGZGVUtPRWY2TDZqd0RTY3JoL2dIRDh6TzIxanY4bzkvOEpoc3JFV2t5OTFaY29nalh5T25KMnN2TTg4ak83bmxJMFhrTFVFSklsVFlXZHRJK1N6VS8rL0JqVmpwdDNuenRKUUtSWm1veS9NMjhFUkNuT2tlWXorT1k4WGpHY0RLbFA1NnhzL09jbGRVMWZ2cnpUNWpOWnJiTVYxYXBpWklFS21CbGRaVTFzWXFRZ2toS0dyV1FkcWZOYXFkRnA5V2kzZ2lKb3BBb0NOazc2dkxCaHgveGo3NzdiWm8xNFd6WUdxUFQwblByLzQ2MUp3WVdwV0JOUjdtU2tzRk04Ky8rL0VjdUdxeWtkRnR5anZ1bUhpOHF2Q1FHb1FJTUVVc0tUbEdCaHJKNDlRaHZoVWRsT0o3ZDVwbitRSWp5NmxKUTlRYVlYRkNTYWNEelY4NnJjR1RldmhmdFN3RXlsWlcwUXVIWmpRdDVjaGErVWFTOUZNbytrMDNMWFZob21URzNvTXlobE9SYWVadUtYN3FyQ2tSNUxyQmtwM09SQW1ESnYyOUtnVENMYTlWbG5JVmxYOHlVTWlKTGJ0R2MwMmIvSjU3TzJYdjZCRE9mODhiTHQvak9OOTZpRVZoNGg2bmMyNkpDei9XVG9ZVHhCckE1ZDY4UUtoa1BZU2FkWHNOU3BoUVRvL25waDUrd3RiSEJuUnVYRVNZaE5kSTVOZ01TYlpqT1lpYVRLZjNobUxQZWtOT3pMcWZuNS9TR0k0eUdNQXBwTmhyVTZ6WEc0ekdOWnBObXEra0dlVlprVnErN3ZyeFJwOW13TTQxR0ZCSUdsZzJaYllpRWxHZ1VqNThkOE1uOUIvejJkNzVCSTdRcjBid2E5T0xmU2dQY3ZOb3NhekxJWURsUzBSMU0rYmYvNGE5UVVUUFRBWmlLN2RmMzdac0x6RHVpM0VNS3MzeTl0OVJkVkYwdFZtM0QvbzFrcXJ6bFNqOWNYbDM2UWFDbXV1NHlvbXp0OWxLT2NqQ280OTFWWWtoemVwRmZ5SllvQ1VLV3pNZ2xwZ0dVY3QwV3pxa3Y2TktyVTMvaEtTejl5YnU1b015djhoeXFJTmVsb1hMVml0K3I2dnpmM1BJelh1Q25PUHZmbjVURjR6b2FERGpjM1lWNHlyZS8rVGJ2dm5ZSFpXSzBMdHE2SWtXbm1MVnAvNU0yYmh2dkxNVENGN0o1ZzFFaGl5b2lkZHhFWkVCcUpKOTg5cEFiTjIreHNiN0MrV0RFZURLak54clI2NDhZOUlkMCsxM1FraUNJa0VwWU9iWlF0RnNkT3AwVlI5K0ZlaTJpMVlqb2RKcDBXaTJhOVloR3JVWXRmOG1keGlDYlh4Z0R4S1FKYnJnY1lOekwvK25EWnp4L3NjOXYvK2F2RUVuUUdleFZsS1hDUlN1bzg0T3ZRS0hoTmRjU1pNRCtTWjkvOTcwZjAycXZFQVFod1VLanhwS0pZT1lyem5ESVpzbExhNVpQZnd0MVlkVUpXSWlOc2gzMm91clA1S0dNcGU5UFZQc0dVVDR3U3VLVkxOaFU1MW9DblZGd2pWazRvb1NoOHBGU3dWWjRyWTB1ME0yRlRiTXkydnV5cGZMbnU1cTh0c2FyQWtURkhGV2FGM3plbkVFVVlTaCt5NlFsd3EyNXhBWGQ0RUllSFN3SjFuUkJIMktaSXRUaTFFK09qdWdlSDlDSkFuN25kMzZERzl1cm9CTjBvbk1xcjgxTEpoL0NadkZjdHQrVnVjOGhiNVd5QThMWnJtMnRwZkw5UGtZUUo1cnBmTTVvUEdVd21YSjRlTUxwV1k5SHV3ZU1oME5TWXdsS1FSaFNDME1hdFlpdHpXMlVsSVJTQVFuTlpwMVdxMEd6RmxGdjFHazBhalRDa0RDVUJjaEVHL3hBRDRHRzFIakQ0ZUw5OEFmZ1JvWjgvR0NIcy9OenZ2dHJYeU1TaGxScmIwaGJxRW54aHFoWjBuUEpFK0twQUxVUVBIaDJ3RjkrLysrNHRMbEZxQ1NwenFYQVpXZlk0b3NzZldaMk9aQkFMSHZHVEdXbHFCZktmckZnQUZqMmduUEJUVVd4MXNrbS9SNXcwZi96c3A1YnVvTkNGeGxFbnVUQmxVMlYrRVgvQzJzL1M0Q0NmRlM5TW9YNHZEbktML1BYc3JsQWVYZSt2RTI0eUI0dHFiSnBzMUpJT0wrOGtCWS9KalFsMjdDdmF6QlV4enZsa05qczI5YkNlR3Jqd3VpbGs0VDkzZWZNaHoxdVhON2t0Ny96VFZiclFEb24xY1hzeE43bXV2QnZaT3MyVjlFcDZjMHhaSkVxbFJoSWtwVEpOR0UwblRNY1Rla05ScHgzZS9RR0l5YVRLY1pvVkJEU3FOZXBoU0hOVmdzcEpPMW1DNTFhQ0dlalVhUFZiTkpwMWVtMFd6VHFFYnZQOW5qbDVYdTBXaldIMm5KclNJZmVzckZvRlIyRnpLaExGaVpyU2N3Nm42SGxQbjJoUUliOC9PUEh6T1p6ZnYyYlgwWG9tQ1RWSHFwZU9zRmJNY2tYd2hlQ1NZZHdzaG9UZ3lDWnAweVNsQS92UCthbkg5NW44OUlXZ2JMYWdwTnVMMnNCTG5wZXpkS0h5MXgwc1MzMUpuNlJHbTJocTJEQnNyYWt2ZkFmZkg5b3RueW81bjJkQlpGTnhSZGpSTWxaNlBQWDg1RE5EQjVxQkRxdkUrU1hYdWg5dVpmZmYzRjE2VGI1VDNhb1pPcy8zenVSRHpBcjRpVC8yNUpGUW8wUUJYdmZ0eitVdm5mM0t4eVBoaHp1UGlQUU1WOS81eFcrK2U1cjFqS3I1emFQSUJmRldMZTdkb3BTTGZLY2Nydkhsb3A1YW9obmMwYnpPWk5wekhBNHB0Y2ZjbnJXWXpRZWs2UzJvWTdDaUNBTUNaU2kwV3pSYnJjSnBBMWpxZFZDVmxzdDJ1MG16V2FEWnIxR1BReHc3Yml0RkZQTkxFNTQrbVNIVisvZG9WbFhkdEp1ZE9XU0szSXpTaXRRN1Vtb3BTbXRyUFBQWE5oWjAwOS85akZoR1BMK2UyOVlSb0x6R2hoVHJHbHRDU1ZSS0l5d0VldHBtakNaelpuTVlvYmpHY1BobUc2dlQ3ZlhaenlaRW12Tk5EWTBteTJpVUtHMTVxemI1eHUvK2gyQ2k4MGkva3RxRnJYN3BncEFMOHdMQzlTUkM1L2FpbkIxV2FzZ3pNWG5VUVZKVmxiVlZaU043dHRWUGp3czU1WXUyVmRqU2hQbzBrOHYvT2p6N0Q5SU1VYitKNm9BL1A4K3ZYQXJzL3pBb0RRUFh2bzVlMzR5TzdSMHJaMHdDL0xlL0N0SVR3UnF5ckxsVWlSbExveHl3b1hHNmdBQUlBQkpSRUZVSUV6M0t6dy9PZVYwZjQ5SUduN25ONzdKcXk5ZGNidDNEVUlobExST1FodU5ZMUZuQnViemxNbDhibS96NFpqQllNaDV0OGRvT0NSMUxaaVVrcWhXSXd5c1dHdGxkZFcxZ2hDR2lrNmpUcWZWb3RhczBhelhhTlJEYXJVYVVhQUl2YXJSSG1xcFJYbHFlNWtOUmxNZVBYN0thNis4UkQxeWtOT1M3RmZrTm5pUjE3dVpEdGFybUROcFBNSUtxYkxlWEVwaW8vangzM3pFeHZvYWI3MTZ4Mm9lakFBWllZeTJROGc0WVRTZE1Kbk9HVThuakFaVCtvTWgvVjZQZURhM1h6TlExTUlhUVdBRFdhTW9KSXdpbTFKMTJxWGRhcEhvbE41Z3hCdnZmcDMvL0QvN0I1WUplRkdmdUFUNyt6bXRBbDh3ZTE1dzZ5elpCSHdlanV6aUY2cFkwWWxGSVV2cHhoY2xpM0ttUWpOKy9iclF0MUl4MzFhK3JzZFpFNEl2L0Y0di9qeVdmQzB2cWJsY0RZaWxCNEFvalQvMTUvNU9GbzFYMGxKNUYzNUh3aXZsUlVWOWNkRnhiZ3FGbXpFYzdMMmdmM3JDOW5xYjMvbk5iN0xlcVpNbXNhVVZHMnVibnM1anhwT1k0V2hFdnova3BOdmorT1NNMFhnR0FzS3dSaGdxUWhVU2hvcTFqUTJrbE1TVE1TdXJIUnIxaUZhalJyUFJvTkdvMDZ4RkJBRU9mK2J5R3pQd0NOWWJZSktVeEFPVUZnSXlPNVVmRGNZOGVQU1UxMTY1UzZzUjJsUWhKOSt0aXF6OCtibjBqWEpDbDZvdGt4RitzUVRxVkV0KzlOT1AyTDU4alJ0WE56Zzg2ek9aelJpTlovUUdRODdPdXZUNmZTYnp4THBHVlVnUUJOVEMwTTRDVk1UNlJ0dTJSQXBDcVlpQ2dIb2pvdDFab2RWYTVTLy8rbTlwTmVzWW94bU9KOXk0OXhyLzFYLzlYNUphSXBDLzdUT1YzbnpKSWZCTDFxRFpuMm44YldydWdGcFF3VjlZSlZRV2cwdmJCUHV5cE42T04xdEpXcmVWRUZtRWx2RlV5cWFVOGxLZC9sOWN5UmdYd1BHNVhMTmZhdEJYVmQ0dGZ0VDZnclZoVmtCVnR5cG1ZWVFwdlYrdFB3Z3NwS3FpR0w2S1lpMmJEeDJOY1ZIWnBzVFNMOThQYm9pYXhPenU3aklmRG5qMTdrMisvZjVYMFBHTXZZTXorcU94TFZXN1BRYURBWEZpR1lGUlZDT01Ja0tsNkhSV2FMVU1TbWdDTjVCck5adTBXazJtc3ptejhZaXZmT050b2xCWm1xNUxFYlpSWXZiRzFUcno2aGNCb0xuMTNNMHFyRlBVeTd4RU1oaE9lZnBzajNmZWVwMUd3TUxHS3FjV2lKSUJ3UUZOS2JJSmhZWENwQnJtU2Nwa05tYzhtVE9aekJnTXg1eWNuWE4wY3M3dWl4UCsraWN4VWdYSVFCRkdFVUdncU1tQTliVkxyR1pxUXF5RU9IVlpqSys5Y3B2TjlRN3RabzBnQ3FrRkVWRWdrWUVnb2NiM3Z2OTNnQ0lNQW9iaktldGJWL2k5Ly9aM0NaU2t2Yjd1YndGTStRR1h4Z3ZWcStZZi94SmRweWgvZEhqR2h3SVlxb3RTMTVTSFQyTHBuU3FXdGduR3o0MHJRRXhGQmtCKzk1bkswbXlaTnVIaUVRY1ZJdXV5eXViTEh3dm1jdzlQdnJCOUtna29MdWo1SzBwTVUzdzIrWlpFZUlHVTNsREVOd1g3Q1pqRlhNQVVlZ2ZYRmtvRTA5R1VGN3M3aUdSR0xWQU0rMzMrNk4vK2U3Uk9rVUZvR2ZaQlNLMFdzSDVwZzBBS0FpV28xVU9hOVFhTldrUzlYaU1NRlBVd1FnYkNSb3dqZWJaL1REd2Q4KzZiZDFIQ29yQjFJaXEyZFYxYSsyYXpuQ3hOMkM2aGl1ZEdhMlB6R0dUSVdYZkVpNE5qM243ck5RSnBMQUpNMjB4QTZ6QzF0akFwTEZjd3l5Sk1OY3dTdzJRV001N2FYcncvbU5EckRSaU1Sa3dtRTFKamlJS0lNQXFKb2dpbEpKdWJsN3pKdmJibGV4alNxamRvTlNJYWpRYTFLS1FXaFFSUlJIK2M4S01mL3kxLzc1dnZjbVd6alRUYWtvS1ZjbUdxZ2trcytOUHYvU1hqYVV4VXF6TWV6Nml2YnZCUC8vbi9oSW9DR3AwTzczemxLOTRXb0Rwd014V25uZmppUXZiQ3pyZEVqREdsOUZLcXBKdlBxVEFXZU1TQ2ttSnRVV0ZIeVpNdlpSWUM0dTJ5elpjLzE3SmJXWnNLVE9PQ0thcjVFcUtjeFUvT0xGUmIxWDlpQ2l4eGFRSnY4bkNWYXB0UUVTMkp5bWNwb01xVW9CejQ3TEFScGxTUGlTclQwZlhSNXlmSEhML1lKNUtHbTljdjA0Z2lwTkFZM1VJSVF4U0ZOSnQxMnEzTW45K2dVYThSaGk2OU5wOTZhM3VEdTlWdHFnV2ZQSHhLb3hieDltdDNMRzBZbThRY0NGa3k2bVJyc2V6N3RNTmJuYW03Q2t0VXZqM1NhRUtPajd1Y2RidTgrZnBMZGpkdklCWEtKa0FCY1dxWVQyS21jY3hzRmpNY2p1Z1BSblI3ZldhVEdmTTR4UWhRTWlDS0ltdkFVWXBtczBtbjNYTDNxQ0VJb0JuVm5XQ29ScjFlb3hIVnFJV1NNQXhjK0l2TExjaUVQVVp3ZEQ3Z2IzN3lkM3puVzEvbjBtb0Q0VEQwQnN1amxESmdPb00vK1k4L1pEU0o2YlJiVE9kelJMM0ovL3o3LzR4NnMwWVExWGpuM1hlSjZqV0M4bWpkTE4xOFYrZGF4cFNWYXRWYk9pLzd6ZUlBVDRpcWFOOTQwZHlWQ3k5WE5KVTkxOElYeFB0ZndGTkE1V0RTUUtHRUlsQ0NxQjV3cHBTN0tmUXZYZEJvdjlPdS9uekxYdXVsMTc1Wm92cXI1TTJYcHhibGVMTk15bU9XNkNsTmRlVW5Tc3E4UXFVb3lnNDhyeW9USmJOUkpqMzJOUDkrYXEzM0M5TmFRNXF5di91Y2NhL0xXclBHMjIrOHhFcXJZU0dadFlBb0NKMGp6Y3RySkl2cVNqRnBtbCtFbWZnbkVKSlVTclNSZlBEUloxemYzdURxMWdvcVYzN2FHejExMFhJRlhjZTJhSklzZEZhNDFDZmNKYUFkNnIxNHh2YVB1Z3dtTSs3ZHU4ZGtPbUU2aXhsTlp3d0dZODY2ZlU3UHp4bU9wcVJhRXdVaEtnd0psU0lLQXdJVjBlN1Vjdml0RklJb1VGYjExMnJRYUlSVytWZXJFVVlCUVNCeis3aXY0alJvcE5ITzdlZ1NpNlJFaUlDajh5Ri8vWk1QK0szZitEYXRtZ1RqOUJwSTEzb29laFBObjN6dkI2UmFzTkp1TTUzUG1hZndMLy9nWDlCc054QkM4dFd2ZjUxNm80YkcrRVBBNWFYcHNybVcrSUk1Z0NqQkNrMUpoN3hzeTVkREVCYTBBcFYvVjFibVVqNUNQRXZNTmVWL0tKSUV6QXl0VTg0R1BaNC9mb2dTS21mTEcybVd6eDR2TEVOMFJTRDV5eHdqb25ManA1NFFTaXgrNXRsZGJySTV2ZmE4NTVveU92M3p4YittSWhrMlBtcE1aQXhFV3gvYjBsamlnMXFLQ3JIczU4alNiZWZUR2ZzN082VFRFYS9jdnM2dnZmOFdyWVlDclYwSXB2Tmt1RERZRXUvVkdHS2hjL2VsWlJPNXFrMG9ZaTM0NlllZjhPcmQyNncyUXFUUXVaczBQNkJFV2Z5Q0MzdkpEeW1IMHRiQVBESE1aZ25qZWN4NFBHVTRuZExyRFRrNE9FWUdBUjk4ZEI4bEJFRVVFb1oyczRDUVJGR1Q5YWp1UUtpS0pFbFo3elJaVzJuUnFOZmRoaUVpRENWQm9JaWtMTlI1T3MxcHY0NDNoRWw5QnFQdzZOTUZWY21nTVRMZzZLVFBULzcyQTc3NzY5K2kxUWpRYVp4ck1xemhTVEtZSlB6Um4vMElLUU5XMmsxbTg1amgzUEF2LzlVZjBGNXAyWmYvL2ZkcE5odDVSRjl3WWJYK0paN3JRbllyeWdJZFdid281ZXZmazZQbXZhZFhraHJQM1ordnBBcnlyUEF0ZjFuYWppMStiT2lGZ2Zsc3pMZy80T3pra0xQREF3NmY3M0t3KzR6UFB2bzVMeDQvUUtBSmxDekxYci9zak40czREWXUvTGZOaFUxQlZYYnJpNGVXLzl1TEtCRXVrQk9iTDlXa21RdnpyN09YUmVVdmRxbGdjeUtoZkJ2aUVuUDd2UzR2bmowajFDbmZldTlOdnZMcUxRS3k2UEpDN2VsRFdvdzNTeERlZ3l5RXdVakxZQkF5WURvemZQRGhKM3oxbmRkbzFpeGExNlNncFNVRzR3NHJleERhb0ZiaDNJTnhuRENkeG95bVV3YkRNY09SM1kvM0JrTm1reGxHS3JzbFVBcWxGR0hkcmdaVnEwRW9CRkdvcUVVQnRVWkV2VjZuV1hQT3ZVYWJnK056UnYwZVgzM3paV3Y1MWNaS2NFUlpzWW5PMmk2VncyZ0wyN3BaZkF5ZDVrSG52eTNGd1ZHWEQzNytNZC85enEvUWloUkd4eVVtUm9Ma2ZCenovLzM1RHpBeW90VnNNSjBubkEvSC9NSC85cSs0dExHT0VmQ1ZyNzFQWjJYRklzbmNMeVVReGwrRGZia2hmNUh1NnltOVBMVmFUbHdReStURkh2akJlQ3o2L0Vvb1FDTGEyTXhBSWF3TFNtb0xsVlJHazh6R3pFY2pldWRuZEkrUDJIMzZoR2NQNy9QMC9tZnNQWDZBU2VlMjFIUGZpd0pXd2dDalFrYnpKTTlZRjJZUiszM1IzUzVFMlg5Z1NodU1KWDZIQytjTHB0UW1pQzh4SXlodGFMSS9RMzl4TlViRkFiRDRsU29IVjJaLzlmK0pUMXB6TEVNTFJURWM3eDl5ZXJoUEl4RDgvVjk3bnh0WDFsQW1kbmtCYmdncmhGZmxGYzJsek9PcUN2MWxEbVVYaXNFbzRmNmpKM3pqdlRjSUE3dW1GRXFocFVTbk1FOFQ0alJsT3BreG1zd1lERWYwaGtNR2d4SGp5WVFrVGxCU0VkWWlJaFc0M2pyZzB0bzZZczE5UFdWTDlWelNHd1hVYTNiZ0ZvVUJVYUNRS21OTnBDQUNQbnR5d0d3MjQ1M1g3NENPbllJUlVyY1J5RkthdFM0Y2p4bDRJUE9NWklkQU5xL0paZW1lMzBJTHhiTVhwOXkvLzVUdi92cTNxRWMyc2NuT05ISUNKV2U5S1gvMDczOUlHRVIwR25YbWNjcFpiOER2LzYrL3ovYmxEWXd4dlA3T1YxaFpXM0dKMk1WakVCaGhGZ2VBWC9nd2xmb0JGam0yU3pCVXBteU1LYnZSM2N0STFxdUJFcUNNWmpvYk1lcDNPVDArNGVSZ243Mm5UM24yNkFGN1R4OXo4T3dSSW9rSmc0QklDa0tIOGQ2b0s0eG9XR0ZFWlBmSHRUQWdDQUltczRTSHV3Y1h2ZUZmdE5QTWQrMCt5bnZaSHliTlJVcG5VVHBJb2FJUEY4dDFBWm1SeElOSVh5QURYandzRnBzTGNhRyt3Z0hJeTNNSlE4a1JLaEdRcHV6dTdERHFuWFA1MGdxLy9XdnYwNmtybEtkYU1INTFJL0ZNcXRwTjAwWEpVcUcxSUhFUlppY25RNDdQK3J6MXhtc01KeE1tMHhtajhZVGhhTUw1WUVDdlAyUTBHak9iMjdWdkVHUysrb0JRS2RiYWJYdXJLMGtZS3BvTnEvU3IxU01hOVJyMVdrQVlSZzRENXJXQk9pMDZTSmtGcG1RSldBRWZmZm9FR2RSNDcrMTdtSFRtWGx4VjNBdE9zYXl6ellxL0tNTGtsNExPZlFBUzRVaFh1RFcxTnFDTjVOSE9JWStlN3ZMZFgzMmZVSmxNUytRa3hSSWhRNTRmbmZQSC8rRkhTRldqMld3UUp3bkg1ejMrK2YveSs3eDA5dzZKMXJ6KzVwdHNiVzhWMjYvOG9wTmVDMUF4MFZ5WUVtakthckhGUVpoWm1NMVpXRUtXN3lZZFZNR1diSUcwZGtrem56SHNubk4yY3N6aDNuTjJIai9rMHc5L3pzSE9FL3FuUitoNGJpZkUycEpjbzBCeGM2M0pwZldyMUFMSnh1b2FxMnVyVEdZeFI4Zkh4S2ttMGU0MmN4a0dKbzFKVTAxaURKSDVmSm1SV0xLR00wSlVodlRlRVdZMEMzbDFDKyt4S1hwM04raXF0bEhtb2svZVVPN2ZQMmMxS01WRnlvSnFxeUM5T1lRcGNRUjh0MTMyZUxnUVhlYmpLWHZQbnBLTWgzemwxYnQ4NjkzWENVV00wWW1WNzJiUGhUQjUySXp0NXdWYUMxQmgxbE9RSkFtekpMWUt0MG5NWURTbTJ4dndmTytBV2hUeDBTZWZJcVZFdVJjOERHelozbTYyYURXYXBLazlnSlV3cks2MmFUYnJOR3MxYWxIZ25IaVJIYmlKeWlqWFlGVi9HUTlSVytrdi9qZ3FqMVFYb0JRZmZ2eUlWcXZHRy9mc3phL3pRWmN1NlNmc255YzlVSkwvWEdSRExKMnpGNHpQN3plQURQanN5UjdQRDQ3NTdxKzlUeWpzUUxERWZwUVJqNStmOE9jLytHc2FEZnR6SjBuSzBWbVgvK0dmL2pOZWUvVVYwalRsN211dmMvbmExZktXT05mR1MzOElLRXVuZmpGTnJkejkvZ3V4SUFmMmhqQVp4VlFZN0JKRlFCS1R6R2YweisyTGZ2VGlPWWU3ejNqNDZTYzh2ZjhwczM0WDBqa0NtNkVoUkVvdERMamFhYkxTMmFEVGF0S28xMm5VSTJwaFFCZ0k0aVJ4Q1RLU3ZmMFhUR2V4KzBXcVhKRmx2Sml0MUhoWkIxWDdlMFdEVUVMWFpvT3l5a3AwRWJobFBtYzFXTFlxYWVPQkpMK01mTGcwUnhXZnEvVmZ3aGxhTGc3S1Z3NWU1TFhRQ0MzS0JETG5hT3VlZGpsNThaeWFNSHozMTcvQnEzZXVRanF6RnQ2ODQzTEJhVUloakNEV0Zwd3htOHdZVHVaTVpqTXJZKzBQNlBWSHpPWXhTRWtZS0lJZ0pBeEQxdGJXQ0FOSkZGcGxXeGpZSElGV3M1SGY0clZhamQyOUE4YmpFYSsrZkl0YUlQTWpOQjgwaWhTaHRhTUQyQmF5ZE1SNTJRblNjU0V6M0p0eEwzU2FHSDd4OFFNMjFsZDU3YzVWVERyUGcweHlzS2hJYzVDTnlRZWxPREVTZVJKMGpzN0Q1eGRrZkVHRFVDR2ZQWDdCMlhtUDMvaVZkeEVteHJqUE1QdTlwVnF6dTd2SGovL201M1JXVnFpSGRWSmpPRG5yOGQvLzN1L3g5dHV2RWFjeHQrNjh4TTNiTjYxT29BTEtRVXAwbkdRSGdBM1FMTXdodXJ6NDloNmxiRCtwblE5ZE9PQ2pGQm9sc1I5MkhCUFBKdlRPVGpnN1BPUjQvd1V2bnUveTlQNW4zUC9GQjB4NzV5aG5JaEd1cDZuWGFtdzFHNnl1WEtiZHFsdDJleTJpVll0UU1sdHphQmVtWUl2dzZXVEszTUI1dDg5a09zOHorb3lQYnhMU2dVd0xYNERKNWFBZTZjcFVWbUo1S212bGxSR2ZMMGV1S3FZWEZjMGk5MUtZWmR2TUwvUUlpZVZTbnhLWnQ5cjNmd21aZHFaY015NGFNR3Q1TW8rRjBlenZIakE2UDJHMUh2SVBmL05iWE4xYXhlaVVWRmxaNmlTZU14aE9HSTBuOUlkaitvTWh3OEdJMFhoTW1tcVVWRVMxR2tIb0FKVkNzcmE2YWx1K1FObVhQYkxyc21halRqT3EwV2hFMUdxS0tJeXNCRmU0Q3NkSWRuWmZJSFRDbS9mdW9KVDF3V1hiRFpVSGJRckhic3p5YXJKbmdyeEN5ZUFEdVkvQnpiRVREWE10K0x1ZmY4TDFxNWU1ZTNNYnJlUHlqRVQ0SzFOL2FLcHpoQnhLSWp3WlpxbTZGQ3EvTElSVTNILzhuSVBqSHUrKzh5Wm4zUUY5Tjd3Y2pNY01oeU9tNHpHemVZS1Jra2F6UTZ0V0o5RndjdGJsdi9qZDMrWDlyMytOT0kyNWR1czJMNy82YXVXeXRtME1TRVNxK2ZSbkg3aG9zTkpKWHlrZTg2aGxON0RSMXArc0FCTW5wTE14dzM2UDNza0p4d2Y3L1ArVm5XdXNadGQ5MW4vcnN2ZDdmOS96bmpObjdtUEhqajIrSkhhY0pqRzlLS2hWQVVFcmNhOG9WVk9hVWxRRUVZVUNMWlFpaElBQWhRcFVKRURBQnhEaUlpRzFTQlVxRXBkS1RkSWtUZXpZVHVKYm5QSFlIbzluUEpkemVTLzd1dGJpdzFwNzc3WFBPR25KbDVtTWJjMDU3OWw3cmYvbGVYN1AxZGRlNGMzWFh1T2JMMytON09ndXFWUW81UThHNFN6ak5PSE0vb0w1ZE1wb1BHQTBIRElNeEZJVjBEeWl5ZWdUQW1tRC9iRjFvZ25xMHBJWFc5WjV3ZkYyUzRQbnRjNTVNa3l6ZXFISko1QklKNml4ZlVSMUw4Rk1kQWp0QmhnaFJLOHFpRldOMzM1VThCNWx2SURXcWhrQ0tUb2dTYng2c2IrckFXeFAyQk16R09NNXpPOWluTnMySGdFczUzcEtRZjkzbWJMaTJwdHZVbVViNXFPVWozL1h4ekRHOE5KcmIzQjR0R2E5MmZoNDlkcUFETm1GeW9OVjA4R1E0V2hFWFpiTUpoTW1veUZwcWhpUFIrMndMZEVDM1dDem1yb2IyOEpvaGJCWVU3WkEwdG9KWG52OUNzUEJrQWZ2djlEbVY3cW01Uk1DYWJ2eXpnVTdkL1AvT2tLdzZLK1ZiWmZzVVR0SGFRUy8vWlVYZWZDQis3bDBabzZ4cHN1bmtIMU92NGhMZkNmYk5zaUVpOFlLVHpjM3hsRVpRMWxVRkdYTnRpalliZ3MyV2NabWxYSDM0QUFwRmYvci83eUxWSm9tY0VJb1ArTWFUK2FrSTh2aGFzMWdNS0MyanR1SHgzemZIL3BEZk0vM2ZDZkdHVTZmdjhEbFJ4L3REcWRvbWl1RXdGbkQ2eSsreEMvOWxiK085Z2dpRlVJM1RMc1JrT0VVbGNKSE9lZWJEZHZqWTk2OThRNDMzbnFUcTkvOEJ0ZGV2OElicjd6RTV1QVdTb1NFWFFkYUNnYXA1bHpndHM5bU04YmpBV21pR0EwR1BzRVdnUW1VRStGaXlxN29lcUVUakgyTFk3VXV5SXVDYlY1UVZGVUhFRVdFdFZCejhuYWV0d1pMM1kyd1hOZnpFcSttT25SMVJ6UVcvOStDSVhGU3loc09Ga0dIRExkQ25CZyt1dC9CWUhOaXlkaWFzcnFwY3Y5d0VOL0NOZmhlOHdYUjdzcmJ6eTA0ODQ2T0Q3aDEvVzJFTVNUQ29TVTg4K3h6REpJRW5UWkJwWUxKYk1aU1NsVHFGWENEUkRFWWpCaVB4bHk1K2lZWExwN2g0dGw5cjJ4cmMvQWFoVnRFeUJFeXNQeGxWeTRiaDVVV29UUjFZWG50bTFmWlA3WExxVDN2K21zMEZLMUFNNlEyaVh1RVVMU3NBaEVPZkJ1a3l3MU5XRXFKZFk2cUV2ejJzOC96MktPUGNucHZpblFtS3FQRFV4UVJqMXdJUmExclEya01lV0Q0YjdLY2JWYXdXbTFZclRkc3R4dXF5cWNCSzZYUmlVWWxta1JyaGxwejZ0U2VCMzdpUXM2QlB4d0h3d0hEMFpoVlZ2SDhWNy9HN3U0ZVRqaHUzVDNpdTc3MysvbDkzLzk5V0Z1eFBMWFBZeC80UUpzZTNPdjU4WW5JMTErN3d0Ly82Wi9GWEgwTGpUTm82NE1EdEhOa20yT09EKzl3KzhaTmJyMTluVmRmK1RwWHYvRXFOOTU2Zy96d0xsSTZWRGp4RkRDU2t0MjlIVTd0TFpsTlBPOXNNQmlnUTdpbGN3N2pETWJVSk1xblk5b1FyM3pTNGlKZCtFSEVGS0VnNDF6bi9wVE1pcHFxOGlJSXFUb3liTGZPYkRESTBZTW1iSGNyQzl1VzRTMWZvdW54MjRxaG1RQVJRa08rZlhkKzc5UTlka3phanRvaStyeWVrNitpN0JsSVhWZVZSUGR4MzJ6ZGhVU0tTUHNleDVvM0w4ZTl1Ry9SVDEwV0huRXVtNUxmR3Q2OWVZdTdOMjhpbmVIUzJWUHM3c3hRQ29ZNllaQnFMM3daRFJrTUVrYXBkNm1KZ0VmMllSYUNGMSsrd2dQM25lSDBjb3B3RmRaMFpiSVVBbWVqM0ljVFdZWE45MkREakdlN0xiaDY1U29YTDUxblBoN2lRa2FCdHgyTHR2K1dyaisvNlVnNTRoNUFxUXNCTUlvdXlydXlrbWRmZUpHblAvSVVzOGtBWnd4V0tPOVlzWmF5cXYxTW96SnNOMXRXbXcxSHg3NUUzMlpiNnJyR1NUL1BHRGFEUzZVWkpBTUdPNmxQaXdvVlphSUY2VEJoUEJ3ekdRd1lqZE1BS3ZITS95WkV4QWpKellNdG4vdnk1NWpORmlncHVIWDNpSTkrOThmNXdSLzRBeGhiTVowdmVQekpKN3pTc3FFQ0I2T1NDeFhPOGJ1MytjZS84UGZZdnZ3S0R5em02SmUvOEJsdVgzK2IxNy94S2xkZWZwRmIxOStpMm01OHoyMXI3NkRETWRTYTAvTXg4L25jRCtOR0huNDRIQ1NjMmR0REs3OGFzaUhNMFFMVzFPUmw0UW04VW5uQVFZdnU2bytzL0duYythaWJ0SmpLT2c0T0Q4aUtrc3JVcllXMzFYZmJUazNTN21yYis5Uy9ITkw1Z1pTU3pnZHdSak82SGs2ejRmZUxXSjMzYlc3NkNQWGRDbWZ1dVcrYkZWSzRONEx1WEo1WXlibDQ4T2dhZTY3clk5T2kwajlhQnZiNlVYY0NoT0xpamNiSld1SUVDbEpJSDFncWJjM1ZxMWZaSEs5WWpBZDgvT21Qc3B3TkdXaS9VbE5DSXF4dEJ2bmRDQ0dNMEowVTFKWGttYTk4blVjZnVwL2xmQlJtT1AwanowWk8wZDVoU2tQNXRmNVpzbzZEb3kydnYvNG1EejF3UDlOUjJsbHlveWw3SXk2VFBTSnltQUhFK296d1cwTURCNVYraHk4a1dWSHk3SE92OFBEREQ1R1ZGVGR1MytYd2FNM2hhc1hoMFJIYklBVkd5SGFPNFhBTTA1VFpiTUx1YUlTS05sNUpFQk9OaHQ3dk1CajZ3ek5OUENmUXR6NjBjSlZtUktpRWFLc2pLeVR2M0RyaWYvN2Z6ek9aemttVGhEdUhLeDcvam8veVIvN3dIOFRhbW1RNDRvTlBQZVZqMnR2Y0RkcllKWWRoYy9lQVQvLzgzK0htRno3UHBmbVVaYW9Rajl4M3ppV0o5MHdySVJpbW12Rm94R1E2Q1QzYWdFRVkwTWdXTTFSeWRIekVmREZuUGhuMUhuUVppS1ZaV2JEWmJKak9aa0dRRTFsUWU2TFlNRXkwQVVnUmZrTFdDZFpaN2xWYlZRWElJT0cxZ2VJcnV6MjZpN3QrMnBBTVp6MHR4VnFIcVExNVdiREtTKzV1U3dhVEhWUTY5QTlIbTRiYmxldS9rM1VuRmdPZDFPODNyWXNRTWRvOGhIQ0s3cmEzelVIVy9ENStJWnhmRmJuM0hPSzdQai9COVRwUXVzU2JxRDkxSjNJWlhLOWdvTXcyakZMSkl3Ky9uMis4OUNMWlpzTjlaL2Y1N3U5NGpIRWlVR0ZJSE5TdFRTb0M3YmNVOXZjb3pTYXZlZUdyTC9IazQ1Y1pEVG9UVHZ4TWlxanphQkczamZyVGR1WWRLeVMzN2h4ejljMXJYSDd3ZmthcFJpbUIwaXF3OGYyOFNBUlhubTJNckwwRnEyeTlBYlcxUHU0OUwxdUt6bWFUc1ZwdldLMVc1R1dKRkFvaDhJZWgxa2dWTmtwU2hFckIvMTFLU3JJc1J5dTg2V21ZaENBUDdXRWNXcE9veGpYb1oya3VyQXhGTDdRa09yU2xyL0prYUttdDFGeS9jY0QvL3N3WEdVNFdKSW5tYUxYbHdVYy93SS84eUEraHRVQ25BNzdqTzcrTDBYQVE3ZnJqZENSQnRsN3h6Lzd1cC9ucXIvNHFFeVY0Y0xFZ3FXcjBFKysvajhsMDdFVVNXclg1NGRhRmFPYmdCYmY0dU9ham8yTXNqdjM5UFJJVmJ1QkFxblZDVUZzNFdoMGpoR0N4MkNIT0YvUzltR3hITWkwcnpub0VwTFFTcENRckNnNk9WMnkyQlZWdFNOTWszRFEyaEk1MWZIcUR3OVRXcDhmV05XVlZVNVVWWlZYNVBEZ3BVV0dGbEtZRFppcmhZRnVlbUl6TFZrQnpUOVMxYzVGd0tacTRmeHZYM3oyL3YwZGhHTXI1R0hQZHFpUHA1ZkIxNFJaZGU5U3pKTk1ZWCtLV3Z0dExkMFBQcm1wdzFyVVZTTU9aeDlhVXVlWFZyNytJcUhLKys4T1Bjdm4raXloWElWdmpqY1VHMFlwdC9zd0tVT0dRa1pyYkIydStlZVVOUHZMVTR3dzBHRnVIR3lnK0FacjlpdTBRNTZLcmVwb2tLQ0VWYjc5emkxdnYzdWJSeSs4bmtSS2tiNm44K3QvNk5sRDQ3VTl0b0tpTlArenprcXdzeWZQU3F3UTN2a1JmYnpiVXhzOFRrc1NETlpUV2FDVVpUU2JNRnp0Qm15TFFXcEtHL2p3SnN3MnRnem93U2JuNjFnMlNCSjcrMEdNa0tseEJ6WGx2blErUnRxYjk4MmJ3MkIzeS9TcXhBWnI2RTlZQ0NXOWN1ODFuZnV0WnBvc2xhWkp5dEZsejZmMFA4Y00vL0NkOFpKMU9lT3BqVHpNYURucVFXaUtOUXAxdCtRLy80bC94NnEvOEtoUGhHQ2tWMXV3T2ZlSDBMa0tKb0tYM2lqd2J1YlNjRkZoaktYUER3ZUZodVBYSHZnUnovbUZyRkNKRldYTG43Z0hMblIyR3d6Umt5WFVTejFaMFFiZi9icThCQjBWdFdhMVhiTEtjOVRaRGFjMWc2RWttUGp2ZVo4aVhsWWNsbGtWQmJYelVjcUsxQjBQb2hPbGtUSklrNFpUMnZaMlMvbGJabEthTmhPNU45cjlWdXMwSjVMWno3cjBGUks0L01CVGkza2gwRVlsZVphOHNiMTdHYnBZZ0xOSFV1bHZKOWplQkovUGlYYy8vN3F4UG9qWFdaOUdYcGc2M2tHdXo3YjBLenJYemg4RW81WHQvNzBjNHR6ZkRocGFyTzVUQ1RDS2s0N1NVV2lFUUt1R05hN2M0T0R6a28wODk3bjMwem5qS2NFaithYzFGN2E4cWFyTnNtSjUzMkt3M3J0MWt1ODE1NU5ITDdVZGNXK0d0dUtXaEt2eExubTB6TnB1TTdUWmptMmZoOC9Bdmh5K3hCVnByZEpLeXV4eUVTa09TYU1GZ2tEQWNlSmZlWkRSa2tDWkJaNUtndFFneDdzMTAzMkl0MUU3eXRWZGVweW9MdnZPcHg5REt0YTQ4SnpvbHByUHVYcjJNaSszVDNhQzVSNU9XQ292aTZyWGIvT2JubjJHeHMyUTRHTERPY25iUDNjY25QdkVqNkVRamxPYXBqM3lVeVdRU0hhTDlTdEZXTmIveUgvOHp2LzF2L2kyblJ5T2NGQmhUa1lTZnA3WmhCZVFkVzgwcVRMWXZxYTBkQjhmSG1OcHcvdHpac090dnRweCs1V0dCdzhORDZ0cXdmM29QSmFSLytFUWJvSVIxalNESHRDV3hNWTZ5cktpcUdtTXQ2MjFPVnRZY0hhOUlkRUs5TGFpcUE4cXFBb1FYaWlpdkJCc09Cc3duSXgvZUtYWDQyajM2MjkzanhRM09NZHZnbWlOOWVrd1AvbDB3Q1ZvVjVEMG0zcTQxNk4zOHhPbkx0RGpuMkdMYkhESE41MFI3UDBiY2cvQjcwL1Q5UWVIb3JHZkdPMnQ5YjlyOEdzck4zbERTK1pXcWRLQ2tJRW04V202WWFoSUpsODZmNGVIN0xqQWRnRE1tT3JSOGJsN2pTKzgyTHNKUDUrV1FsNzd4RmxvS1B2U0J5MGhuZkdWaHV3T3E1Mk1NbVhWaEV1YWZlU25ESWVXb3JlUE5hKzl3NS9ZQmU2ZjIrT29yVnpoZWJkaHVjNHF5d0ZoUXlpdjgwZ0RLU0xWaVBKa3huYzVEeUNjb0pSa2ttc0Znd0dnUU9JQURMeXhLZFlwU29lOFdIZVNVNEZ3VUdGOFloYlUwZ1NOaG5PU1o1MTVDcDVxUFBmVUlxb0hNdTI0ZzI1cWNJalQ2eVJsbGYvaVFBQUFnQUVsRVFWU25jQjFlTFo3UE9PZW9uZURxMnpmNTdCZWVZM2R2ejlOOHNwekJmSmRQZnZMSFNOSUU0eHhQZnZncFpvdFpleWpUYW5uQzUyNGN2L1lydjhhdi9jTi93bG10T1Q4YmMvTjQwNnBGdmVCT3hCTlIyeXFsaEJCa2VjWEJ3UUdMblFXenlkZ3I0Vm9CZzVkUFZ0Ync3cDA3ektZemRoYkRidFVWWlhIVTRXUzNDTXJha09jNTY3VlhnSlZsVFZHV2JEWmJxc3Bqb1FacENzb3hHZzZZejhabzdhTzR3V0tNQ1lPT0pqQTBjaUpZZHhJZEVGREtJVFFpQ2p2dGhYVzBZWjBkaXk5R2ZzZnBjNzZIK3hZZXZGNkNVcGZGWm52Ty96ajVKOHJLQzErUHdudmJteGJING53eXJYVlk0N2NwMWhtdjFyRzJiYi84c2pubzFrTmY3RXRjeFRCTi9iNDk4UzlLMDVzbVdpRnhMR2NqenB4ZU1reVRObXV1YlhPQ3k0NG9aY2UydzBlTHN5a3ZQUDh5Wi9kM3VYUnVEMkVyYkd0Zjd0SnlFZElyMm94djA3S2laSnZuYkxLQzQrTTE2L1dhelNhanJHcVVraDZtb1JLeTY3ZUNYa015bVV5WlRpY0lLVkhDVzI2bDhBYVpTK2ZQTWhuNmpMMDBUZENKUUNrLzFQUlZyUTFPUnR0dW1GckFwNHVTa2lLTWVlTS9iM0lNeXhvKy82WG4yTnRiOHNTakR5Q2N1VmY4YWJ2bjNva1QwbzZtdlFtaUhCR0Y2RFlYaDVPYWI3NTlpOC84MWpNc2QwK2h0V1pUbE5oa3lFLyt1WjlnT0JwUUc4c0hudndRZTd0N0FXY2ZpTU94ODk3VS9NYXYveC8rdzkvNkJjNHB4Wm5GSEdxTGRjWWozY0xYcG1XZi8rRFBNd2VIUjBkWTV6aDc5alJhK2RPL05iZ0VENzIvclkvWVA3V0hVcko5d0oyUW1OcFNWaVZGV2JMTkM3OHVXYThweThxN2tIUkNrcVRvUkpNT0Iwd21ZN1FLOGNXTmJDcjRvUkdDMmxSK054cFdpMTBtU1JSeUZZa2RSR3RHY1QxWmpIWFdmNCt4NWwxMG1RRHhiUjNIWjhXbnRPMlpkMlJrY1BMckx5ZWFDK01FU1M1cUM3eG94ZC9XMXRaWVkzRFcrcGZjK1Fqc2xnTWdQSHUrS2UyVmNHanBiNzgwMGQ3b2xHaC9FeXB2aGxIQlBxMWFoNThOT2djUHhKU3Vaam5aNGV6K2t1RkFoNkJKWDErNDRNQnNFM3hsMExRRlJIVWIzZUVrTDcxeWxRZnV2OGplY282elh1U1NseVY1VlpGbEpWbWVzOXA0VmVCbXZmSFZuSkJlVWFvVld2a2VXeVZEbHJzanJ6MEpyVnVxZk1XWEREUXFTUmlHMVZpUytoSzlyQ3l2WGJuQ1ErKzd5TjVpR3JJQzZTSmR3aEM0T1NSdDc0ZlJqNW4zYWxFUmhZMTQ2Yk1Nei9PMmNIenVDMS9tMG4wWGVmemgrOEJXSFdETzljTjBZOHZXdDB4U2M1MFdvaW5mTFlxcmI5L2hjMTk0anNWaWwwUXJ0bmxCWmdXZit0UlBNcHROcU9xYWh4NTVsTFBuejNaSjlFMjFRWGVZZk80M1BzY3YvOHhmNTR5U25KdFB3VmhxSWZvZ1V5SFIwdmtReFNZT2E1dm5IQjBmczdmY1l6NGVkZVFXMGNFZ25IUGNPVGlrcUdvV2l4Mk9OaHUyV2NsbXV5WExNb3FpYkY5RXJaUy9lUkxGWWpaQkp5bGFKNTdJWWwxa21Zend5ZTI5NlA5SldSWklxZEF5aVNTNzhZY2RCWS9FRVZZdDhscTA3SHBMeHhGd2NUbE5GMFBXajFRSy8xeTZhSS9lOWZJdDdDSzY2cHVNUWE4c3MxaHJRbGtlYm5GYkJhR05hQ3JMSUtuMnY2WlMrQnRhS1liRDFLK01VazJpRWorZzBoSWQ2TFJPZEZIWklsYWtPZE0rSU0xQVVBcGYrdS91TFRtOW5KRXEzY3A4bXdDVk5yR3BEYXp3WUJDRDhNYWRxbVpiVkdSNXhhdXZYV1U0R3BGZHVjYnErSmdzMjJJcUExSzI0cFlrMFNpcFVWb3lueStRd3Nlekt3RktxYmFFSHd4U2hvUGc1a3Y4SWFhbGFxZmEvbUF5dm5WMGNIaTA1dXFiYi9QSTVRZVpUNGVkSzFQNHRXL0Q5WmV0MVZKR2N3OFJXY0NiaDhqN0gzcW1XT0V3U0k3WEpWLzgwdk04OVBEN3VQekErUkFYVG5kcGhJTy9IZlFGakZmbjNZakVGbEdWYUYybndMWkM4L1hYcnZIRlo3L096czR1YWFMSnk0cXNoci93cVQvUHFiMGx4bHJ1Zi8vRDNQKys5N1hWSHlkSEN3NmUvK0tYK0tkLzZXZllxUTJYZHVha3RaLzdJTU5sMVNaODQ4TkJCUXByYSs3Y3VZdlVpbk5uejVLcXhIUFlha05aMTVSbFJWbFVyTGNaNjgyYTlYcUxsQktwYm5oVWNhclJTakdmejBrVFRWbldHRk9USm42dE9FNVRsRlpzOG9Jc3o5dThkTm4yM0tJblFRVkJWUnZxcWlCSkJ5aWxXcUpybkREYk45TjA3VXVmRnVRVEFhcmFlUHVvRTRpZWF1NEU4YWI1T2tROHFyWGhnSWhnSk03Nlc5czZyS245MnNyWWR2TGJ1ZWxNUjM1eGpsVDU2WEo3bzJtUHkwcVYzMWdvSmRGQ3ROUHc3dHlKTzJvVDJIWWkydi9MdHZ1S1o1QUNSNm8xWi9hVzdDMFhTTzBETDBTSXRXNTI2TVk0akFuUldVVkJsaFZzdDFtdzRXNHA4bVlIN3RkaldpZmtSVWxkMXlSSnduQzRpd3FjZnFVbGlVcEl0V1F3SEpDbS9nWlBVMi9MMWxwNnlLY1VyVEpUaGd2QU5SNFRhMEtiRjl4ODBpUEU3OTQ1NUsxcjEzbjhzY3VNUjdvVld3bnJPanQwR3gwbW8rUTZGUVdaeEhyOWFGRVRaZDg2SWJtN3l2bnNGNS9seVE4K3hnTVg5M0hoNW8rSHJiMkV2Qk1nYmRkeUc1ckVhZGtPMTcyM1JtRFJ2UERpNjN6bGE2K3d1N3VMbG9xOHFqamNsdnowVDMrSzA2ZDNNZFp5NGI3N2VlRDlEN1o2aTBoWTJyb3VYMzd1cTN6NkwvNFY1cHN0bDNibURBTWgyWXJPZ045OEd0STV0SkNTTEMrNGZ2TVd3OUVZSVJUWHJyL3JCeTVGVGxXV0tLWFFxV2FRRE5GYU1abU9tYzluYUtsUTJpditrc1FQNGpaYnYyYVpqQVlNaHpQR3d3RktLcklzNTg3aGtROXo2RWt6dzIyRGpTYnRrR1ZicEZJTWg2TStNa21Jd0s4UE42Mk1wdDQ0Yk8xTHZicXVLT3VhcXE2cHk1cXFNdFNtb2hJU0t6cTNWelNEai9iN3JsdUROb00xVTNXUVNtUGFyNlVWbmpoL0N5aUI1dyttUGxzdTFmNno4VXg3alpMK1lmWjhPdEVLbDVyUHc3VUhqWXZvQys2OU9SNjk5azNTV3lxRUVuTSttWEo2YjVmSlpBVFdVVlFsMjNWR1dWUmtXeTlWWFcyMlpIbE9XZVFZNjFCU294TkZrdnBFSGFVVTQ4bWMrV3dlRU5wZXhwcWtQZzl2RUY3c1JDZWsydiszV2lsVXExdTBQWU5TUndReU5HMTBHTHY1ejhSR04yZDdDRXFja055NWU4aTd0Mjd4eEJPWEdTYTZnOHEwWjJOUVhGclQ2ZjFqQjJ2azVteG1HNjRYK05vUlV1OGU1WHp1UzEvaEk5L3hCT2YyNXQ0Q0hBUml4cmtXSlI1UDNac0lkUmUxQUcyOFpaTkxHVm9HSVNSbDVYaitwVmQ1NWJVMzJkOC9qY0JUaFE5WEdaLzZTMytSOHhmMk1iWGwzTVVMUEh6NU1rcllIZzVLeU03Z2R2VzFLL3pEbi9sWkJqZHU4TUQrTHVQR0M5R29RTVAzYXgxWVkxR21Rai83dFZmSWlwTGFPWFN5OXRaTHBkQmFNWjFPU05TY0pOR2tBZU9jYUYvT05ldVZoZ3VYWnlYSDZ6VktLWWFESnBnaElhOHFqbytQcUNvZkJOSFRZN3RJc0JJZUEyTU1XVjR5R3FZb3JYc3ZnWk4rZ2x3Ym43QmFWLzRGcjhxS29pckR5K2tmL0dhWU9FZ1RSck9objJNb3lhcXNXYjl6MjZPZW5jSVppN0ZoaW03cVFLRU50N2pybTJYQ0pZU1N3cGVzb1JkTkU5K1BhKzBoa1ZJNmI0VnRtZzdub3VyR2hvZkVScHVLeU9NdllwR1BqTnhydHZlZytwV1RqSVJQdnAzeTM0ZWxOblhnMlZWODdlVnZzTjV1S2ZMU0UyV1U5OWMzTDNlaUZZT0JEN3hVVXFLMEpra0VxVTRZRHNKS0xQSHd5eVNVOWtxS2lLL2Y5YlAwbk8vbVBlZ0lJcndRL1h6bzJKUXA0dCswSlkzaStzMWJySTVYZk9DeFIxQXF6RVhhbGFMcm1BM0NiNnJvNVVPSWJyZmkrbmIyRTA1OUhIQnduUEdGTHovUDB4OTlpdFBMQ1VLWURpN2ZTTVJkSERNZkkyNGtSZ2hxWS95NnVqUVVWY1VteThseXp5SGNaR3Z5YmM1bXV5VXZheGFMSlltVzVGWE40VGJucC83Q1QzSGZmUmN3MXJKN2FwOUhIbjg4TUEyYUY3bkIwZmc1MmNHTmQvbkZuL3NGekRlK3lmMjdPMHhhS2J6QVNnZkdpNUdNa0JqQU9NdkZVM1AwN3U1T0U1aU9VakxJRTFXd2JBWmVtb2dtbU5GaXgxbW9qV1cxMldDdFF5dEZtcWFrd3lIR0dPN2V2VXRSMXFIUGxVSElJaUw5ZmF4WGx4U0ZYL0dNUm1PTXFTbUxpcnIySzZXeXJDaXFDbE5YV0dmRGRGZVFEbExTTkdVOEdaSm9qWlplS1lZUWJWNXFrK2RuclNVSkw0MHBNK295YTNYa1RVbXZnaDUrTVBEQ2o2Rk8wRnFGSGx6NUFadnN1RUE5OVY3emNsb1hML0hhUFM4OVI1cnJxY0c2a0E3UlEzRzFjRTZaaEhiRFVwb2FVMXVxMm1EcW1xSXFxY3NLYTYyZnVXaUYxcG82U1RCRmhkYUt4V0lIdFJzc05zcXIzTFRTSklrTTVoMnZvMGhTSFE3M3hqRHpIbTVrYTRKL3B4bWdkUUdkTWVHbzd6bDBmWmRtQ05DS1MrNE9GaVRDRGRrZ3RCUnZ2L011ZVo3eHlPVUhTUUp6MG9XYnIxMU5OcTJhN1JTQnJ2ZjNkaE01cit6cnRNRU9HNEk4SlFmSEdjODgvM1UrOXZTSDJaa09ROFVwZWhXRjE5YjdOcldvYThxaXBpZ0xOdHVjOVdiRDBkR0cxV1pOVmRWZUg2c2tRaWtTN1NYQVByMW53bUE4NWVEdUlhTlJTbGxaampZNVAvckpIK2Z5NVllcDY0cnBmTTRUVDM3SSsyK2EyNzhaVW9ZZnlQSGRJejc5YzMrYnpWZWY1K0x1aklYU1NGdjdTaGZaejdzVUVtZEw5aGNEenN4UzlOblRleUdyVEp3b2g3dnBwc01pbWx6M0lOeXh4cEpsT1hWdGdpTU1oc01oU01IUjhUSGJMR3ZsdFRHSXhJWWhrNitzRFZWdHFLcWFzdklUWStNczFqaVVFUDREQ3pITmcwSEtkRHBHUzRsVWl1MTI2MWx1d3lGeFpIYnNySE11L3Z2RG9TQmdxQlU2VVF5Yk1qMU0wYlZTQVN2bVY0N2VXMlREWUtpNTVTeHhTSjZJTlB2aWhBaWowU01JMTYwZ1hTTjVsYm9iZURaREsrdkM5KyszQVVWUU50YVZQd2pyeW05Q3BQSXJzQ1R4VXUzRmRCS0VVSnBVYXc5aGthSVZSeVZKVTY1clVpbVIycS9SUkl4NWo0WUh6V2FsWTNWMVZHYnBSRTlMM0I1c2tlejRub2dVMGIxb1FnVFRscE50YTlQOUtxTDJ4ZjhjTEpLM3I5L0VBcGN2UDRBemRmaDZveVMrYUFMYlp2VzFSMHpYbERzYlVxS1JXTkd0ZHYzM3JiRUlidHc2NE91dlhPR0RIM29TcFNSM2pyWVVSVWxSQm1wUm5yTmFyOW1FalpaRW9IVDRiRU1RcVZLU0pFblpQN1hYeW9CbG9GWXJyUmtOVWtiakNYbGxlZW5sVjVuUDVwU1Y0WENkOHlkKytFL3g1QWNmeFZyRGRMSGd5YWMrUkpMcXJrNTAvU1NvOWNFeC8vaHYvVjF1ZnZhelhKeU0yRXNVc2piWVdIUGFtRnVzcjVyR1NuQnBPVU9hREsyYnRWcXovb2dLelpOQmtRSkJaUXhaV1dCS0V3WXp6WnBYc3M0eThxSUlXQ2lOTlphNjloTGlxcTZwcXBxcXJxaXJtdG9ZdndwU0ltd0pFcFk3Ty83QlZzcHJvbFUzdlc5dUdtc05SMGZIektaajBsVFQwaDVjZjVBWVUzYzdNWTFnT2t4NTVQN3pMYU5PT2h0SmdCMkMyanZVb29kRHVJNlIzM3dTcmhkMElrK3dEMjIzamd6VmpYRW1lQk1NdFRWVWRVVlZWZFNoaGFtcUt1Q3RIRXI2R3p4Sk5JTTBaVHhJU1hTQ1ZqNG9nc0MxSDZRSms5R0k0WEJBVWRhc2oxZElET2ZQbkdJeUhyUTNlTE1XYXpjQ3pYcVY3cUJxTVdKUkVuS3NweERDOHhhYVpKMklwTkk3SzJRRVRlbFJqQ01xallpam1sM0hEblR0eE40aHRSY2dYM3ZuSnNQQmdITm56MkJzRlRsRkk2MkZhRXplY1pLeGJBMWp3bmxTb1JQT28rSktTMVlVYkl1U3FxelpaaGxINnpXYmJjYlI4Um9ML09ablB1K1ZjbUhXb1dVNFRJT0lhTFM3NTlzbEtiREdNSnVOMjBOMmtDZ0dlb0JPRlZwSmtvWjFJRHRaMXQzam5DOCs4eVZtOHpuV3dhMjd4L3p4UC8zRFBQWFVreGhuR0k0blByd2oxY1JCdFM0eVZHU3JEYi84NlYva3JWLy9kYzVOaHB4S0U1VHAwcGxzK1BtYlVOMVlXNU1JeTFCSk5CYUpRN2Q1WTBRV1RSZEJLcHd2ODR1eTlQcDZaNUZhZTBsaHNFY1dlVW1XRjVSWlJWR1g0VVd2d1JnL0xXNTExNXJSeUE4U2xReXVzc0NoZDgyRDJyUUZ6UnFOcHAvMlV1UHRaczF5WjZkRmhEY2FlZit3bWxBZWRjWlpRUnhxMllpZFJDVFlhQkp0Yk1Rbmx5ZFExaUxnbXYxRDVQZmtzaWNQdG1IYWE2M3hQYmkxVkZXTkRSVk9WZnREenpubnAveGhucElraXZITVUzS1U5QnNBR1dpMWtmbTh6UUp3MWpFWkR6bTl2MlE2R2lJRkhCNXRXUjJ2bVl4SG5EbTFKTkVpOFBoRnJBTnMvZmV0cjZLWGNCYmRsQkVncU8yUTQvUmJSMC9JMUppUm1vZk44d003aVhlbk1XbXFRTmRXQTUwejFQWEVNcFZ4dkhQelhXYXpLYWYzbGo1U3pLdi9jWTFKS25qd0haS2lydjNoV2h1S3lsZE9lVkd3elFxeXJHUzd6Y2l5akN6TGNjNjNxMG5ROXJjdGJ6cmc5T2toRXRHYWZWTHRkUWlEMURQL2ZZSlJFclkzS1RkdjNHUTJHN096bUlmSzJYakhlVXhhRDFaY0s4Qll3YlYzRC9uc0Y1NWxzZGpCSWJoKzV5NS85RS8rS1Q3MjlNY3dWWWxPRTU1ODZzTU1Ca2tFWm8xL1BoWmJXdjc5di9wM1BQOWYvaXZueDJQT2pFYW9jRUM2Y0RqTFlLWVNWcUFWN0owN3hkekNuYmZmYmc5ZUxXaXNpNnF6U29hWGNWTVU1TnVDb3F5b2pLR3VMVlZkZWlWWDdsOTBhMnFra3I3ODBacGhtakllajhLZ0tQakxwYVMvczNnUEVLOFFQVTkzMzc0cVdXODNsRVhKY3JrYi9NNGlVZ3JRc2dCNnFUZHh6b0RyYmdaT2dEVTZ3WExvNTJPa2QxQ0JOVE9FMnZoaFlWVWJmM3ZYdmp5dmFyOEd4TG93TTBqUktwaE1SaW1MZE9vVmpjRkoxdW9xZ2d1eVE0NjUxaVJrbTliRytpcGtNaGx4ZW0rUGFST09nZVAyN1dPT1YydG1rd21uZHViZTVOcUx4b3FqM2lUZGlGMUVvRk1iTVFPQ01NclNEdEZrVENBS3Q0b1VvdTlLZEk0NE9NeC9GcjYwa0VFdzFTamZaUFJpTkE2KzdrOFV4c0sxZDI1eTlzeDVack1obGJGVXRmZURaSG5CTnN1OWt5OHZPRjZ0MkdZRm0wMUdrUmQraXlLOS8xNHA3OG5Yb2VmMkx0Y3BXaWkwZ2pSSW9kT0dUSlRvb0NLVVlkYmpLVlVpdkI4aS9HeWFBK3JHT3pmWW1jK1l6MGE0RU5UUkRqOUVQMWV6VVVhK2NlTTJ2L21GNTFndWxtaWx1WDI0NHZmL3dBL3lzWTkrQ0djcWhGSTg4ZFNIR1kyQ3N5OVNzaUo4UExrekZmL3RQLzFYL3RlLy9OZWNTaFBPVE1kZzZuRGppeFBzU1ljVWp1VWs1ZW5MRi9uOHEyK2loV3RuTHJvU0lJekQxaVZGVlZHVS90VDBvcDZjdXZZcExEcEpmQmthYnZURlBDWFJ2clFSelErNW1iVTJMM0NQRUN1K0RVZXZLYmlsSDhTNFJvbm1KK2JIeHl1a2t1d3RkMXB2ZS90cUM5Yy9KVVU4U1pjbmhwZlIxMktETUNKc0k1eVRHQnZhRmVOZmFtZUNCcUt1TUpYZkVJaXdrMWFKdjhGSDZSQTE4ZE4wblhnNmpwSysvS3lOSWR0dW1FNm1ZZDh0V21wcy9NeUxTSFBnblpnTnROUlRleGZUS2FkMlpvd0gzanZ1S3dIQnpUdDN5ZktTL2IwOUpxTzBSYmU1MktVZzZHZCt0ZDJTNkZDd29vdGNhc1ZFSWdvQmpaS0VuWE1uVXA3cEowbkgzTWt3ODNHaTRkbjduNGNVQWllOVROaFlMMzIyTlJSVlNWRWJqbzVXWkhuTjNjMWJyRmFIYkxlNWg4QVlGOXBHWDRwTDdUY1dnM1RFSUIyRjJDOGZKNVltbWlUMUxNRWs5Y0NTSk14K3ZBOGdYSHhoTTlQTUJqejd2eGxNdWpaL3dRbi8yVm9wcVVyRHV6ZHVzOXhiTUowT1c5TlVZMjF0UUxSTnEyUHhwTjgzM3JuTjU3LzhBc3ZkUFZLbE9OeHMrTTZQZjV6dis5N3Z3VGxMVlZWOCtPbmZ3MncyamRLWlhQdVpPK2NCcDcvKzMvOEh2L0tMdjhSZW1uQmhNa2FHMUNIL2Q4b2VrRVk1UnlvTlR6NThrVVJXYVB5Z1d3bC9pT3N2ZitWcktPRlB2Q1RWU09YM3VkNGROVUludWswL3RkYUVsNkRMOFd1bTJjMXFxL1d5dDdhWFNFMTNRaEFaNDdjUTk5cmVqWUdqb3dPbWs2bjNPb3V1TEk4andKdWRkMFArOWRWRWMxQTBlMUtCc1E1anZQR29xdnpMWFZVVlZWbjZ3WnN6U0NIOWhEd01COU1rWVRRYWtpaS8rL1kzdUhlNDBjcG1pUWk3RHVzTVpWbVJaem56K1J3aFZDLzhvZkduYzZKSmlSSHJRc0RPenBUZDJaUmhNK3R3RnB6Q1dNR05kMjlqZ2ZObjlrbFVRRk1MR2V5OUo5MkM3Z1M4T2NvYWtyRlpvVXVCanRlTHplUzdvVHhqNmJuWUd1Q0p3R3NzbXV2RldHOEg5ckp3cnlMMDB2Q0tiWmF6V1cvWlpGdktzbXJQYktVOU1VZHJMd3ZYU2pKSVVtYmpzZGNZQkJhQVgwY25iRGNibHNzZFR1MHRQTlVxY0N1a0NCbVFJWTRNRzJjd3loNTh3enJiOVNkUlZIcXpKclJON3FBUWJGYzVod2VIbkRtOXozQ292S1E3cGl2WndBdTBRVEtQd0FuRjFldTMrZTFudjhydThoUlNTbzdXV3g3OTRKUDg0QTk4djY4TnBPYnhKNTlrdWJQb1ZjdXVwVWY0dHU2M2Z1TTMrVGQvNCtmWmM1SUw4d2tEYS9vLzRUQnJRWUMwamdHR3kyZVhURVNOY01FaEd6RWk5Tm45ZllacFNwcG9sSUpVSjE3SnBmd1UxZ0dyN1piTk51dTdsMXBYbk95VWNZS3VCSTl6NXUvNVg4VFE3eEZoWEx2dnpyS0NJaTlZN3V3Z3ZaY1hHNzVaZjFLSGdZandaV05RMnZvKzBCaE1WWkhYRlhWdHFhdXlYU2MydzVRa3hEeE5oa1AwZE55Sm1xVHFFb1JGWjJHbXJVcGNqNlhmclpGbzdjL3JiUVlXNXJPZFZxQWtYWk9JYWFMUDBVWURSSDlZS1FYTDJZemR4UXl0UW45dFRSaHFTWXF5NXVhdHU2U0RsTE43TzhobTV1RmNCRFkrY1pzNzRiYzRybE9rMGRDZmpZeW0rQkZZSkJMaU5MdHRJY04rS0ZDaGErUENGcWNJRmwzL1ltZFpUcmJOeWJMTTZ6OGNLSjJFS2ttSHo5Z0x5SGJtY3ovMFZMSURmYWdrYkMvOE5IMm9OV21xQWtCVWhmbVA1SzNyN3pJZjczRGgvQm1jOVZablhCME9uOHBicWs4NGNseUF4RGJUZEwvbGtwMHJ2Zlc2TlZ1YlpuZ3BXUjE1YU1qNUMyZElkQ2pIYlo4VjBZYUJOSldsVkZ5NWRwTm5uM3VaM1owOXBGWWNISzI1NytITC9OQVAvYkVnSmhJODh2aGo3SjgrRlhsUkltcUw4T0VoWC9ueXMvenp2L3pYMkhPQ1M3TUpnOTVoMzNFUmJmQ1NKTlE4ZEdhWHBhWkYzU2toQWpIWlAzSDYwbm4vemVoUWtqVlZ1WFdPNC9XR3crT1ZuMDQzKzE3WFY1cHhJZ0ZZZk10QTdMai9seDJIUDU1c2hsL1c2eldKVGxndTV4RUwzV0NkQ3krM0Rac0ZELzd3L1hnRjFpR2xEQ3VaQkoxb1J1TWhXdnVVR0dNZHg4Y3JKdU1SbzhrSUdSR0ZXdU5RdStHelBTeVl2OFZONitacUlzVmNuTGZuNFBoNFJUcElHWTJIQWYybDJwQkhFYkhJWEtDK2VPZWNZWmdvZHBkTFp0T3g3Kzl0SXlIempHb2hCSnVzNE4xYmQxanNMTmhkVEwzMDJMcUlmV2Y3b1NGTnF5VDZyTUN1VHhTZDFrQjRKajVHWUdydlhTanJpcXFzeWNLdFhlUTVlVm15WFcvSmk3QUNGdElidWdJVFVHdHY4a2tTejdiemR1MmdMUWxEenpSTmZGSndFalFMZ1I4cFpTZmpkczFjSWp3MjNxWHFiYzU1Ym5uanpldnM3aTA1ZDNiZk0vdHNSRysyZ1RMam9wQndSL1RwUkw5enRpTW9XUkdvVnpaWWY4UDh3OExoNFlyTlpzUEZDMmQ4RmlCZWs5R2c5T093dUU3dG9YbnR5blZlK1BvMzJOMDdoWktLZzgyYSt5NC93bzk5NGsramxLU3FheDUrNUJIT1hUamZyb29iNEdpbmhSSzgrTUlML0pOUGZZcDV0dUgrK1lKQlRQbzlrZUprcVJtNG12ZWYyV0daQ244NGhuOVhTazkxa29IQnFUM1hyRXNtZFFMeXN1THV3VkdnN2hMdHF2dGNxWmJtMG1QYnVTNzJLMXJQRUNtMXVrMkdESjk5VUUxWncvSFJNVXBwblBQcm1XYXFXOWQrL3l1VkpOVkptRWxvSnVOUkNIalVFZk5QdEpxRDVvYk5pNExqNHcyTCtjSlBWMjFmQ0N5aVBmVTk3cjBZMnhrcjFJSVUwNFVkNjkyREkyYlRHVHBSTk5WMVc5NExUbnhldmxVWXBpbTd5eVdMNlFncDdEMEpQVUw2OHUvb2VNdkIwUkg3KzZkOHZ4L29taUlxVzBYWTVRblgxb0J0emw1REQ2NXE1NGVYVlVWV2xPUkY2WTFjMllZc3l5bXowcTl5cmZYYmlzVGYzR25pM1p2T0dZYWpJYlBaTkd4emZMYWVDaTkzb3psSVZmZHlOeTVQSldRRTUzTTlySnB3SFhra0p2ZzJBMlEvRkJWczFqbFgzbmlMOCtmUGMvYjBIcWF1SXhtM0NQcUNiazl1MjdXdDh5WWgwWm03WFpSdzNEM2F0bVU3YXVFNWxuZnVIRktXSmVjdm5rWEtRTzBSSFQyNUFaazA4eVl2d0VsNDZjcmJmUFdsMTlqZDlUZjcwWHJENlVzUDhHTS8raU9rcWNaWXgvc2VmSkQ3M25lZjMveUVNNzlKQnhaS1lhM2x4dFUzK2FXLytyT2tkMjV4YWJGa0lnS3hxdzBaamQ1Q1p4R3U1SDM3Uy9ZSDJzLzNoTUphMlZxUFpXTjhBM1NqWVFjL3REbzhPbWFiNTBpaG0wQzZUcy9kQzhqc2d5KzdSTlRBZEpQeHlTaW9uWjhoMU1aaWF4TUlQNkVQTDJ0cVU3YzdjS1dObjBsb3pYZzBZRDdUcUZENktSSFhhcTUzbW9zZVdLSGpyaDF2TnVUYmpOM2xEbHJyZUw5M0R6NDc3c083MzlvMnRiWHp0M2ZUaXJxMkhLOVdMSGJtbmx2WUhHNG5RZDBkS1lUUktPWFVjc1owT1BDYkV1ZDZTeExSemprRWQ0L1diTE9jOCtkT2syclovcnhhaGFIenZYOWRXMnh0TVhWTmFReFprVk5XRlhsZWtlYzUyeXluTEV1TThUUmRGU3k1S2tpL0I4TWhzL0hVenptVWFHM0ZXZ20wVnF4V1c1U0E4MmZQb0JMbFhYMVNlays1ZUs5czVKQzIwL2cySWdWaDdLY1VRZ1ZWb093T2hiWUVOKzNQWVowVnZQSEdOZTYvL3lLbmx2Tmd3T3FTaWpzbkptM1ozb0JtdmQwWFpCdkdhZHZabExmK2krWnhhZzhwaStETzNTT3doZ3ZuVGlOVmcxQVRrWWdyMmlZSk1EaXNUUG5tbXpkNTRhWFhXTzdzb0tSZ3N5MVpuci9BSjMvOEUraFVZeTJjT1grTzl6LzBjQ1JqanA0LzRWZStkMi9jNUJmLzVzL2pycjNGaGRrT1V5bTgrN001Y01LaDM1S0lNRnhjTGpnOUdualJsSkJlTGl4bEc5c21vMW1RbGdJcTV6aGUrVlJWWHhhcVB0MUdpQlBsL1ltRW1oREtVVnYvOEZuci9ONjdxcW1xa2lwazhybEFmbWxreGxvcnhzTVJhdUlmUUM5MGtaMk0xd1VTallpejFVVEVrdXQ0YXQzTEtkdHdVdXNreDBkcnJEWHM3ZTc1bFk0N0dkRDFPMlJ4bmVpdlJEdkQ4Ri9YZHB0VFZxWFhKdlFTaG15WEgraTZZM0kyR2JHL3U4Tm9rSVl5dFpIRXVxaWhrS0I4RjNEejlnRmxaVGkxdTB0ZVZSeHRTckxDa0JjRlJWR1NiZjB3TFM4TGFtT0RWMEcxYWtGZmJ2dHR6WFE4UXMzR1h1b2J5bTRQQ1BHck1xVTFxZExvd0ZGVTB1T3JqYlc4OWM0TnhzT1U5MTA2MjU4VHVCTTJ1TWg4MCtydlhUUVhibS80V0U0dGlMMWg3UUVkL3R4YVdLMEx2dm42Nnp4eStVRjJGaE12aTQ3cXQ5N0lPV0l0eWxiMDFEQU40dHlIMkVKOFlpdUQ1TTZ0T3dnbE9IditUQ3VFRXNKck9icVpvcVNKTEJLaG4zL2oraTJlZWU1RmRuYjhxbStiRjZTekhUNzVaejdCYUpSaUhTeDJGeng4K1hKRU1YWmRxTGIwQjh6aDdUdjhnNS83Mjl6NjhqT2NINDFZYUEyMmFzTnBtL2V5K2N5VU01eVpEYmswRzZPRDVaeG9OdFdzc3YyN0xCRklkSmFYSEt4VzFNYmlVQzMwc29WY2hHL2NXWDlxZXJHRmoyV3VxNkRxQ3owajBqOThYc21ucU1xUzJYeEdvalZDZ2xZNnJBeWIwZk1Kb0ZZYjZSWDNzczNOYUx0LzJ4STlRTkFiVzRkN3h4akxyVHQzR1ErSExCYXo5dWx5djZzWC81NTBVMDRZUEJIQzkvdEtLUmJ6ZVcrUVJ3UXRkYzYzSlR1TEticzdDMGFKYm04TXI5TlcxTlpRVnFZRnFCUkZ5WGE3WmJYeUlFdGpIYzVlYVJXWE11eTRmWnFTWmpLZE1wVlRmMk5yU2FJMVpWVXhVSUxGWWs2aVpOUm4rME5XOXJqczNWQzJjYWsxaDFtZVY3eisxalgyejV6aTNQNE96cGsyMmJhTlR1aUJOVTRFcGdmY2VFOEpKT0t4cjR6UGpOWW5UNUFNV3d1SDZ5MnZ2L0VXano5eW1jazQ4UVl1QkZJRVE2L3c1aGFKYktXSVFzUlBRemhJR2taQ21FRzEyYzZ1OFFHR2N0ckFyVnUzbWM0bTdPNHUyckpwZUJNQUFBQjZTVVJCVkdyUVdFdHVhc3E4Smk5cjhyTDByc29pSjhzS1gyVnRjNDQyR2N1ZFBkSWtZYlhad0dESVQvelpIMmM4R1dPdFl6S2Y4WUVQUG9HV1VhS1Q2K3pCNE5nY3IvamxmL1JMdlAyNXozTXFUVmdrR2t6ZFBvdU4wYWR4VitJY2UrT0VCNVl6VWxPaG5MZGllUmVsaCtBMDFHVWJvSzRJK0grNGE5TUF6d0RQMlFBQUFBQkpSVTVFcmtKZ2dnPT0iPgo8c3R5bGU+CiAgOnJvb3QgewogICAgLS1iZzogI2Y3ZjVlZTsgLS1wYW5lbDogI2ZmZmZmZjsgLS1pbms6ICMyYzJjMWU7IC0tbXV0ZWQ6ICM2YjZiNWE7CiAgICAtLWxpbmU6ICNlNmUyZDU7IC0tbGluZS1zdHJvbmc6ICNkOGQzYzI7CiAgICAtLWFjY2VudDogIzhiMDAwMDsgLS1hY2NlbnQtc29mdDogI2YwZTdlMDsgLS1jaGlwOiAjZjJlZmU0OwogICAgLS1hY2NlbnQtMjogI2E1MjEyMTsgLS1vbi1hY2NlbnQ6ICNmZGY2ZWY7IC0tYWk6ICMyNTZiM2M7IC0tYWktc29mdDogI2Q2ZTdkYjsgLS1vbi1haTogI2YyZmFmNDsKICAgIC0tY3VwLXNvZnQ6ICNlNmYxZWE7IC0tY3VwOiAjMmY4ZjRlOyAtLWN1cC0yOiAjMjc3MTNmOyAtLW9uLWN1cDogI2YyZmFmNDsKICAgIC0tY29kZS1iZzogIzFmMWUxODsgLS1jb2RlLWluazogI2YzZWZlMjsgLS1vazogIzNmOTE0MjsgLS1oaXQ6ICNmZmY0YzI7CiAgICAtLWstdGFibGU6IzBlN2M4NjsgLS1rLXZpZXc6IzdhNGZkMDsgLS1rLXNjYWxhcjojMmY4ZjRlOyAtLWstdGFibGVmbjojOWE2NTAwOyAtLWstYWdnOiNjMDVhMmE7IC0tay10aW86I2IwM2E2ZTsgLS10eXBlLWNvbG9yOiMwZTdjODY7CiAgICAtLXRpbnQ6NiU7IC0tdGludC1ob3ZlcjoxMSU7IC0tdGludC1vcGVuOjE1JTsgLS1lZGdlOjI0JTsgLS1lZGdlLWhvdmVyOjQ2JTsgLS1lZGdlLW9wZW46NTYlOwogICAgLS1yYWRpdXM6IDE0cHg7IC0tcm93LXJhZGl1czogOXB4OwogICAgLS1tb25vOiB1aS1tb25vc3BhY2UsICJTRiBNb25vIiwgTWVubG8sIG1vbm9zcGFjZTsKICAgIC0tc2FuczogIkludGVyIiwgc3lzdGVtLXVpLCAtYXBwbGUtc3lzdGVtLCBzYW5zLXNlcmlmOwogIH0KICBAbWVkaWEgKHByZWZlcnMtY29sb3Itc2NoZW1lOiBkYXJrKSB7CiAgICA6cm9vdCB7CiAgICAgIC0tYmc6ICMxNjE1MGY7IC0tcGFuZWw6ICMyMDFmMTc7IC0taW5rOiAjZWNlN2Q2OyAtLW11dGVkOiAjOWE5NDgyOwogICAgICAtLWxpbmU6ICMzMjJmMjQ7IC0tbGluZS1zdHJvbmc6ICM0MjNlMmU7CiAgICAgIC0tYWNjZW50OiAjZTI4NTdmOyAtLWFjY2VudC1zb2Z0OiAjMmMyMTFmOyAtLWNoaXA6ICMyYTI4MjA7CiAgICAgIC0tYWNjZW50LTI6ICNlZTlhOTQ7IC0tb24tYWNjZW50OiAjMWExMjBmOyAtLWFpOiAjNWNiYTc0OyAtLWFpLXNvZnQ6ICMxNjIzMWE7IC0tb24tYWk6ICMwZjFhMTA7CiAgICAgIC0tY3VwLXNvZnQ6ICMxYjI3MWU7IC0tY3VwOiAjNmZjZTg3OyAtLWN1cC0yOiAjODJkODk3OyAtLW9uLWN1cDogIzEwMjAwZjsKICAgICAgLS1jb2RlLWJnOiAjMTAwZjBhOyAtLWNvZGUtaW5rOiAjZWNlN2Q2OyAtLWhpdDogIzRhNDIyMjsKICAgICAgLS1rLXRhYmxlOiM0ZmM3ZDA7IC0tay12aWV3OiNiMzlhZTg7IC0tay1zY2FsYXI6IzZmY2U4NzsgLS1rLXRhYmxlZm46I2UwYjA1NTsgLS1rLWFnZzojZTY5MjY0OyAtLWstdGlvOiNlMDg0YWQ7IC0tdHlwZS1jb2xvcjojNWJiZmM3OwogICAgICAtLXRpbnQ6MTIlOyAtLXRpbnQtaG92ZXI6MjAlOyAtLXRpbnQtb3BlbjoyNiU7IC0tZWRnZTozMiU7IC0tZWRnZS1ob3Zlcjo1NiU7IC0tZWRnZS1vcGVuOjY4JTsKICAgIH0KICB9CiAgKiB7IGJveC1zaXppbmc6IGJvcmRlci1ib3g7IH0KICBodG1sLCBib2R5IHsgbWFyZ2luOiAwOyB9CiAgYm9keSB7IGJhY2tncm91bmQ6IHZhcigtLWJnKTsgY29sb3I6IHZhcigtLWluayk7IGZvbnQtZmFtaWx5OiB2YXIoLS1zYW5zKTsgbGluZS1oZWlnaHQ6IDEuNTsgLXdlYmtpdC1mb250LXNtb290aGluZzogYW50aWFsaWFzZWQ7IHBhZGRpbmc6IDAgMjBweCA4MHB4OyB9CiAgYSB7IGNvbG9yOiBpbmhlcml0OyB9CiAgOmZvY3VzLXZpc2libGUgeyBvdXRsaW5lOiAycHggc29saWQgdmFyKC0tYWNjZW50KTsgb3V0bGluZS1vZmZzZXQ6IDJweDsgYm9yZGVyLXJhZGl1czogNHB4OyB9CiAgLmljIHsgd2lkdGg6IDE2cHg7IGhlaWdodDogMTZweDsgZmxleDogbm9uZTsgfQoKICAuc2tpcCB7IHBvc2l0aW9uOiBhYnNvbHV0ZTsgbGVmdDogLTk5OXB4OyB0b3A6IDhweDsgYmFja2dyb3VuZDogdmFyKC0tcGFuZWwpOyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lLXN0cm9uZyk7IGJvcmRlci1yYWRpdXM6IDhweDsgcGFkZGluZzogNnB4IDEycHg7IHotaW5kZXg6IDEwMDsgfQogIC5za2lwOmZvY3VzIHsgbGVmdDogMTJweDsgfQoKICAjdmdpLXVzZXItaW5mbyB7IGRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiBjZW50ZXI7IGdhcDogOXB4OyB3aWR0aDogZml0LWNvbnRlbnQ7IG1heC13aWR0aDogMTAwJTsgbWFyZ2luOiAxNHB4IDAgMCBhdXRvOyBiYWNrZ3JvdW5kOiB2YXIoLS1wYW5lbCk7IGJvcmRlcjogMXB4IHNvbGlkIHZhcigtLWxpbmUtc3Ryb25nKTsgYm9yZGVyLXJhZGl1czogMjJweDsgcGFkZGluZzogNXB4IDE0cHggNXB4IDZweDsgYm94LXNoYWRvdzogMCAycHggMTBweCByZ2JhKDAsMCwwLDAuMDcpOyBmb250LXNpemU6IDEzLjVweDsgfQogICN2Z2ktdXNlci1pbmZvOmVtcHR5IHsgZGlzcGxheTogbm9uZTsgfQogICN2Z2ktdXNlci1pbmZvIC5hdmF0YXIgeyB3aWR0aDogMjZweDsgaGVpZ2h0OiAyNnB4OyBib3JkZXItcmFkaXVzOiA1MCU7IGZsZXg6IG5vbmU7IGJhY2tncm91bmQ6IGxpbmVhci1ncmFkaWVudCgxMzVkZWcsICNiNzUyM2YsICM2YjFkMTUpOyBjb2xvcjogI2ZmZjsgZm9udC13ZWlnaHQ6IDYwMDsgZm9udC1zaXplOiAxMXB4OyBkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IGNlbnRlcjsgfQogICN2Z2ktdXNlci1pbmZvIC5lbWFpbCB7IGZvbnQtd2VpZ2h0OiA1NTA7IG92ZXJmbG93OiBoaWRkZW47IHRleHQtb3ZlcmZsb3c6IGVsbGlwc2lzOyB3aGl0ZS1zcGFjZTogbm93cmFwOyB9CiAgI3ZnaS11c2VyLWluZm8gYS5hY3QgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyB0ZXh0LWRlY29yYXRpb246IG5vbmU7IG1hcmdpbi1sZWZ0OiAzcHg7IGZsZXg6IG5vbmU7IH0KICAjdmdpLXVzZXItaW5mbyBhLmFjdDpob3ZlciB7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgI3ZnaS11c2VyLWluZm8uc2lnbmluIC5hdmF0YXIgeyBiYWNrZ3JvdW5kOiB2YXIoLS1jaGlwKTsgY29sb3I6IHZhcigtLW11dGVkKTsgfQoKICAud3JhcCB7IG1heC13aWR0aDogODIwcHg7IG1hcmdpbjogMCBhdXRvOyB9CiAgaGVhZGVyLmhlcm8geyB0ZXh0LWFsaWduOiBjZW50ZXI7IHBhZGRpbmc6IDI2cHggMCAyMHB4OyB9CiAgLmxvZ28geyB3aWR0aDogMTUwcHg7IGhlaWdodDogYXV0bzsgZGlzcGxheTogYmxvY2s7IG1hcmdpbjogMnB4IGF1dG8gMTZweDsgfQogIC5sb2dvLWxpbmsgeyBkaXNwbGF5OiBpbmxpbmUtYmxvY2s7IGxpbmUtaGVpZ2h0OiAwOyB9CiAgLmxvZ28tbGluazpob3ZlciB7IG9wYWNpdHk6IC45OyB9CiAgaDEgeyBmb250LXNpemU6IDMwcHg7IG1hcmdpbjogMCAwIDZweDsgbGV0dGVyLXNwYWNpbmc6IC0wLjAyZW07IH0KICAuaGVybyAuZG9jIHsgY29sb3I6IHZhcigtLW11dGVkKTsgZm9udC1zaXplOiAxNS41cHg7IG1hcmdpbjogMCAwIDEycHg7IH0KICAuaGVybyAubWV0YSB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtc2l6ZTogMTIuNXB4OyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IH0KICAuaGVybyAubWV0YSBjb2RlIHsgYmFja2dyb3VuZDogdmFyKC0tY2hpcCk7IHBhZGRpbmc6IDFweCA2cHg7IGJvcmRlci1yYWRpdXM6IDVweDsgfQoKICAubGFiZWwgeyBmb250LXNpemU6IDEyLjVweDsgbGV0dGVyLXNwYWNpbmc6IDA7IHRleHQtdHJhbnNmb3JtOiBub25lOyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXdlaWdodDogNjUwOyBtYXJnaW46IDAgMCA5cHg7IH0KICAucGFuZWwgeyBiYWNrZ3JvdW5kOiB2YXIoLS1wYW5lbCk7IGJvcmRlcjogMXB4IHNvbGlkIHZhcigtLWxpbmUpOyBib3JkZXItcmFkaXVzOiB2YXIoLS1yYWRpdXMpOyBwYWRkaW5nOiAxOHB4OyB9CgogIC5jYXRiYXIgeyBtYXJnaW46IDIycHggMCAxMnB4OyBkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDEwcHg7IGZsZXgtd3JhcDogd3JhcDsgfQogIC5jYXRiYXIgLmxhYmVsIHsgbWFyZ2luOiAwOyB9CiAgLmNhdC10YWJzIHsgZGlzcGxheTogZmxleDsgZ2FwOiA3cHg7IGZsZXgtd3JhcDogd3JhcDsgfQogIC5jYXQtdGFiIHsgZGlzcGxheTogaW5saW5lLWZsZXg7IGFsaWduLWl0ZW1zOiBiYXNlbGluZTsgZ2FwOiA3cHg7IHBhZGRpbmc6IDZweCAxM3B4OyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lLXN0cm9uZyk7IGJvcmRlci1yYWRpdXM6IDEwcHg7IGZvbnQtc2l6ZTogMTMuNXB4OyBjdXJzb3I6IHBvaW50ZXI7IGJhY2tncm91bmQ6IHZhcigtLXBhbmVsKTsgY29sb3I6IHZhcigtLWluayk7IGZvbnQtZmFtaWx5OiB2YXIoLS1zYW5zKTsgfQogIC5jYXQtdGFiOmhvdmVyIHsgYm9yZGVyLWNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLmNhdC10YWIuYWN0aXZlIHsgYmFja2dyb3VuZDogdmFyKC0tYWNjZW50LXNvZnQpOyBjb2xvcjogdmFyKC0tYWNjZW50KTsgYm9yZGVyLWNvbG9yOiB2YXIoLS1hY2NlbnQpOyBmb250LXdlaWdodDogNjAwOyB9CiAgLmNhdC10YWIgLmN2IHsgZm9udC1zaXplOiAxMXB4OyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtdmFyaWFudC1udW1lcmljOiB0YWJ1bGFyLW51bXM7IH0KICAuY2F0LXRhYi5hY3RpdmUgLmN2IHsgY29sb3I6IHZhcigtLWFjY2VudCk7IH0KCiAgLyogY2F0YWxvZyBtZXRhZGF0YSBjYXJkICh2Z2kuKiB0YWdzKSAqLwogIC5jYXQtY2FyZCB7IG1hcmdpbi1ib3R0b206IDE4cHg7IH0KICAuY2F0LWNhcmQgLmNjLXRpdGxlIHsgZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IGNlbnRlcjsgZ2FwOiA5cHg7IGZvbnQtc2l6ZTogMThweDsgZm9udC13ZWlnaHQ6IDY1MDsgbGV0dGVyLXNwYWNpbmc6IC0wLjAxZW07IH0KICAuY2F0LWNhcmQgLmNjLXRpdGxlIC5pYyB7IHdpZHRoOiAxOXB4OyBoZWlnaHQ6IDE5cHg7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLmNhdC1jYXJkIC5jYy1kb2MgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDEzLjVweDsgbWFyZ2luLXRvcDogNXB4OyB9CiAgLmNhdC1jYXJkIC5jYy1kb2MgLm1kLWggeyBmb250LXNpemU6IDE0cHg7IGZvbnQtd2VpZ2h0OiA2NTA7IGNvbG9yOiB2YXIoLS1pbmspOyBtYXJnaW46IDhweCAwIDRweDsgfQogIC5jYXQtY2FyZCAuY2MtZG9jIC5tZC1wIHsgbWFyZ2luOiA0cHggMDsgfQogIC5jYXQtY2FyZCAuY2MtZG9jIC5tZC1wOmZpcnN0LWNoaWxkIHsgbWFyZ2luLXRvcDogMDsgfQogIC5jYXQtY2FyZCAuY2MtZG9jIC5tZC11bCB7IG1hcmdpbjogNHB4IDA7IHBhZGRpbmctbGVmdDogMThweDsgfQogIC5jYXQtY2FyZCAuY2MtZG9jIGNvZGUgeyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGJhY2tncm91bmQ6IHZhcigtLWNoaXApOyBwYWRkaW5nOiAxcHggNXB4OyBib3JkZXItcmFkaXVzOiA0cHg7IGZvbnQtc2l6ZTogLjkyZW07IH0KICAuY2F0LWNhcmQgLmNjLWRvYyBhIHsgY29sb3I6IHZhcigtLWFjY2VudCk7IH0KICAuYmFkZ2VzIHsgZGlzcGxheTogZmxleDsgZ2FwOiA4cHg7IGZsZXgtd3JhcDogd3JhcDsgbWFyZ2luLXRvcDogMTNweDsgfQogIC5iYWRnZSB7IGRpc3BsYXk6IGlubGluZS1mbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDZweDsgZm9udC1zaXplOiAxMi41cHg7IHBhZGRpbmc6IDVweCAxMXB4OyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lLXN0cm9uZyk7IGJvcmRlci1yYWRpdXM6IDhweDsgdGV4dC1kZWNvcmF0aW9uOiBub25lOyBjb2xvcjogdmFyKC0taW5rKTsgYmFja2dyb3VuZDogdmFyKC0tcGFuZWwpOyB9CiAgLmJhZGdlOmhvdmVyIHsgYm9yZGVyLWNvbG9yOiB2YXIoLS1hY2NlbnQpOyBjb2xvcjogdmFyKC0tYWNjZW50KTsgfQogIC5iYWRnZSAuaWMgeyB3aWR0aDogMTVweDsgaGVpZ2h0OiAxNXB4OyBjb2xvcjogdmFyKC0tbXV0ZWQpOyB9CiAgLmJhZGdlOmhvdmVyIC5pYyB7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLmJhZGdlIGIgeyBmb250LXdlaWdodDogNjAwOyB9CiAgLmJhZGdlLmxpY2Vuc2UgLmljIHsgY29sb3I6IHZhcigtLW9rKTsgfQogIC5jYy1rdyB7IGRpc3BsYXk6IGZsZXg7IGdhcDogNnB4OyBmbGV4LXdyYXA6IHdyYXA7IG1hcmdpbi10b3A6IDExcHg7IGFsaWduLWl0ZW1zOiBjZW50ZXI7IH0KICAuY2Mta3cgLmljIHsgd2lkdGg6IDE0cHg7IGhlaWdodDogMTRweDsgY29sb3I6IHZhcigtLW11dGVkKTsgfQogIC5jYy1rdyAuayB7IGZvbnQtc2l6ZTogMTEuNXB4OyBwYWRkaW5nOiAycHggMTBweDsgYm9yZGVyLXJhZGl1czogMjBweDsgYmFja2dyb3VuZDogdmFyKC0tY2hpcCk7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IH0KICAuY2MtYnkgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDExLjVweDsgbWFyZ2luLXRvcDogMTFweDsgfQoKICAuc3RhcnQgeyBkaXNwbGF5OiBmbGV4OyBmbGV4LWRpcmVjdGlvbjogY29sdW1uOyBtYXJnaW46IDAgMCAyNHB4OyB9CiAgLnN0YXJ0ID4gZGl2IHsgZGlzcGxheTogZmxleDsgZmxleC1kaXJlY3Rpb246IGNvbHVtbjsgbWFyZ2luLWJvdHRvbTogMTZweDsgfQogIC5zdGFydCA+IGRpdjpsYXN0LWNoaWxkIHsgbWFyZ2luLWJvdHRvbTogMDsgfQogIC5zdGFydCA+IGRpdjpudGgtY2hpbGQoMSkgeyBvcmRlcjogMjsgbWFyZ2luLWJvdHRvbTogMDsgfSAgIC8qIENvbm5lY3QgKi8KICAuc3RhcnQgPiBkaXY6bnRoLWNoaWxkKDIpIHsgb3JkZXI6IDE7IG1hcmdpbi1ib3R0b206IDE2cHg7IH0gLyogRXhwbG9yZSBmaXJzdCAqLwoKICAuYXR0YWNoIHsgcG9zaXRpb246IHJlbGF0aXZlOyBiYWNrZ3JvdW5kOiB2YXIoLS1jb2RlLWJnKTsgY29sb3I6IHZhcigtLWNvZGUtaW5rKTsgYm9yZGVyLXJhZGl1czogMTBweDsgcGFkZGluZzogMTVweCAxNnB4OyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtc2l6ZTogMTIuNXB4OyBvdmVyZmxvdy14OiBhdXRvOyB3aGl0ZS1zcGFjZTogcHJlOyB9CiAgLmNvbm5lY3QtdG9wIHsgZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IGNlbnRlcjsganVzdGlmeS1jb250ZW50OiBzcGFjZS1iZXR3ZWVuOyBnYXA6IDEwcHg7IGZsZXgtd3JhcDogd3JhcDsgbWFyZ2luLWJvdHRvbTogMTFweDsgfQogIC50YWJzIHsgZGlzcGxheTogZmxleDsgZ2FwOiA1cHg7IGZsZXgtd3JhcDogd3JhcDsgfQogIC50YWIgeyBkaXNwbGF5OiBpbmxpbmUtZmxleDsgYWxpZ24taXRlbXM6IGNlbnRlcjsgZ2FwOiA3cHg7IHBhZGRpbmc6IDVweCAxM3B4OyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lLXN0cm9uZyk7IGJvcmRlci1yYWRpdXM6IDhweDsgZm9udC1zaXplOiAxMi41cHg7IGN1cnNvcjogcG9pbnRlcjsgYmFja2dyb3VuZDogdmFyKC0tcGFuZWwpOyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LWZhbWlseTogdmFyKC0tc2Fucyk7IH0KICAudGFiLWxvZ28geyB3aWR0aDogMTVweDsgaGVpZ2h0OiAxNXB4OyBmbGV4OiBub25lOyB9CiAgLnRhYi1sb2dvLnB5bCB7IGZpbGw6ICMzNzc2QUI7IH0KICAudGFiLWxvZ28ubmRsIHsgZmlsbDogIzVGQTA0RTsgfQogIC50YWI6aG92ZXIgeyBib3JkZXItY29sb3I6IHZhcigtLWFjY2VudCk7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLnRhYi5hY3RpdmUgeyBiYWNrZ3JvdW5kOiB2YXIoLS1hY2NlbnQtc29mdCk7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyBib3JkZXItY29sb3I6IHZhcigtLWFjY2VudCk7IGZvbnQtd2VpZ2h0OiA2MDA7IH0KICBkZXRhaWxzLmF0dGFjaC1vcHRzIHsgbWFyZ2luLXRvcDogMTVweDsgfQogIGRldGFpbHMuYXR0YWNoLW9wdHMgPiBzdW1tYXJ5IHsgbGlzdC1zdHlsZTogbm9uZTsgY3Vyc29yOiBwb2ludGVyOyBkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDhweDsgcGFkZGluZzogNHB4IDA7IH0KICBkZXRhaWxzLmF0dGFjaC1vcHRzID4gc3VtbWFyeTo6LXdlYmtpdC1kZXRhaWxzLW1hcmtlciB7IGRpc3BsYXk6IG5vbmU7IH0KICBkZXRhaWxzLmF0dGFjaC1vcHRzIC50cmkgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyB0cmFuc2l0aW9uOiB0cmFuc2Zvcm0gLjE1czsgfQogIGRldGFpbHMuYXR0YWNoLW9wdHNbb3Blbl0gPiBzdW1tYXJ5IC50cmkgeyB0cmFuc2Zvcm06IHJvdGF0ZSg5MGRlZyk7IH0KICAuYW8tY291bnQgeyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtc2l6ZTogMTFweDsgY29sb3I6IHZhcigtLW11dGVkKTsgYmFja2dyb3VuZDogdmFyKC0tY2hpcCk7IGJvcmRlci1yYWRpdXM6IDEwcHg7IHBhZGRpbmc6IDFweCA3cHg7IGZvbnQtd2VpZ2h0OiA2MDA7IH0KICB0YWJsZS5vcHRzIHsgd2lkdGg6IDEwMCU7IGJvcmRlci1jb2xsYXBzZTogY29sbGFwc2U7IGZvbnQtc2l6ZTogMTIuNXB4OyBtYXJnaW4tdG9wOiAxMHB4OyB9CiAgdGFibGUub3B0cyB0aCB7IHRleHQtYWxpZ246IGxlZnQ7IGZvbnQtc2l6ZTogMTAuNXB4OyBsZXR0ZXItc3BhY2luZzogLjA1ZW07IHRleHQtdHJhbnNmb3JtOiB1cHBlcmNhc2U7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtd2VpZ2h0OiA3MDA7IHBhZGRpbmc6IDAgMTJweCA2cHggMDsgYm9yZGVyLWJvdHRvbTogMXB4IHNvbGlkIHZhcigtLWxpbmUpOyB9CiAgdGFibGUub3B0cyB0ZCB7IHBhZGRpbmc6IDdweCAxMnB4IDdweCAwOyBib3JkZXItYm90dG9tOiAxcHggc29saWQgdmFyKC0tbGluZSk7IHZlcnRpY2FsLWFsaWduOiB0b3A7IH0KICB0YWJsZS5vcHRzIHRyOmxhc3QtY2hpbGQgdGQgeyBib3JkZXItYm90dG9tOiBub25lOyB9CiAgLm8tbmFtZSB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgZm9udC13ZWlnaHQ6IDU1MDsgfQogIC5vLXR5cGUsIC5vLWRlZiB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgY29sb3I6IHZhcigtLXR5cGUtY29sb3IpOyB3aGl0ZS1zcGFjZTogbm93cmFwOyB9CiAgLm8tZGVzYyB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IH0KICAuYXR0YWNoIC5rdyB7IGNvbG9yOiAjZDlhNDQxOyB9IC5hdHRhY2ggLnN0ciB7IGNvbG9yOiAjN2ZiOThhOyB9IC5hdHRhY2ggLm9wdCB7IGNvbG9yOiAjODZiOGQ2OyB9CiAgLmNvcHkgeyBwb3NpdGlvbjogYWJzb2x1dGU7IHRvcDogMTBweDsgcmlnaHQ6IDEwcHg7IGJhY2tncm91bmQ6IHJnYmEoMjU1LDI1NSwyNTUsMC4wOCk7IGNvbG9yOiAjZDhkM2MyOyBib3JkZXI6IDFweCBzb2xpZCByZ2JhKDI1NSwyNTUsMjU1LDAuMTUpOyBib3JkZXItcmFkaXVzOiA3cHg7IHBhZGRpbmc6IDRweCAxMHB4OyBmb250LXNpemU6IDExLjVweDsgY3Vyc29yOiBwb2ludGVyOyBmb250LWZhbWlseTogdmFyKC0tc2Fucyk7IH0KICAuY29weTpob3ZlciB7IGJhY2tncm91bmQ6IHJnYmEoMjU1LDI1NSwyNTUsMC4xNik7IH0KICAuY29ubmVjdC1yb3cgeyBkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDEwcHg7IGZsZXgtd3JhcDogd3JhcDsgbWFyZ2luLXRvcDogMTJweDsgZm9udC1zaXplOiAxM3B4OyBjb2xvcjogdmFyKC0tbXV0ZWQpOyB9CiAgLmNoaXAtYmFkZ2UgeyBiYWNrZ3JvdW5kOiB2YXIoLS1hY2NlbnQtc29mdCk7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyBib3JkZXItcmFkaXVzOiA2cHg7IHBhZGRpbmc6IDJweCA5cHg7IGZvbnQtc2l6ZTogMTJweDsgZm9udC13ZWlnaHQ6IDYwMDsgd2hpdGUtc3BhY2U6IG5vd3JhcDsgfQoKICAuZHZtZW51IHsgcG9zaXRpb246IHJlbGF0aXZlOyB9CiAgLmR2bWVudSA+IHN1bW1hcnkgeyBsaXN0LXN0eWxlOiBub25lOyBjdXJzb3I6IHBvaW50ZXI7IGRpc3BsYXk6IGlubGluZS1mbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDdweDsgYm9yZGVyOiAxcHggc29saWQgdmFyKC0tbGluZS1zdHJvbmcpOyBib3JkZXItcmFkaXVzOiA4cHg7IHBhZGRpbmc6IDVweCAxMXB4OyBmb250LXNpemU6IDEyLjVweDsgYmFja2dyb3VuZDogdmFyKC0tcGFuZWwpOyBjb2xvcjogdmFyKC0taW5rKTsgfQogIC5kdm1lbnUgPiBzdW1tYXJ5Ojotd2Via2l0LWRldGFpbHMtbWFya2VyIHsgZGlzcGxheTogbm9uZTsgfQogIC5kdm1lbnUgPiBzdW1tYXJ5OmhvdmVyIHsgYm9yZGVyLWNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLmR2bWVudSAuZHYtY3VyIHsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmb250LXdlaWdodDogNjAwOyB9CiAgLmR2bWVudSAuY2FyZXQgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyB9CiAgLmR2bGlzdCB7IHBvc2l0aW9uOiBhYnNvbHV0ZTsgei1pbmRleDogMzA7IHRvcDogY2FsYygxMDAlICsgNXB4KTsgbGVmdDogMDsgbWluLXdpZHRoOiAyOTBweDsgYmFja2dyb3VuZDogdmFyKC0tcGFuZWwpOyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lLXN0cm9uZyk7IGJvcmRlci1yYWRpdXM6IDExcHg7IGJveC1zaGFkb3c6IDAgOHB4IDI2cHggcmdiYSgwLDAsMCwwLjE0KTsgcGFkZGluZzogNXB4OyB9CiAgLmR2aXRlbSB7IGRpc3BsYXk6IGJsb2NrOyB3aWR0aDogMTAwJTsgdGV4dC1hbGlnbjogbGVmdDsgYm9yZGVyOiBub25lOyBiYWNrZ3JvdW5kOiBub25lOyBjdXJzb3I6IHBvaW50ZXI7IGJvcmRlci1yYWRpdXM6IDhweDsgcGFkZGluZzogOHB4IDEwcHg7IGNvbG9yOiB2YXIoLS1pbmspOyBmb250LWZhbWlseTogdmFyKC0tc2Fucyk7IGZvbnQtc2l6ZTogMTNweDsgfQogIC5kdml0ZW06aG92ZXIgeyBiYWNrZ3JvdW5kOiB2YXIoLS1jaGlwKTsgfQogIC5kdml0ZW0uc2VsIHsgYmFja2dyb3VuZDogdmFyKC0tYWNjZW50LXNvZnQpOyB9CiAgLmR2aXRlbSAucm93MSB7IGRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiBjZW50ZXI7IGdhcDogOHB4OyB9CiAgLmR2aXRlbSAuc3BlYyB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgZm9udC13ZWlnaHQ6IDYwMDsgZm9udC1zaXplOiAxMi41cHg7IH0KICAuZHZpdGVtIC50YWcgeyBmb250LXNpemU6IDEwcHg7IGxldHRlci1zcGFjaW5nOiAuMDRlbTsgdGV4dC10cmFuc2Zvcm06IHVwcGVyY2FzZTsgYmFja2dyb3VuZDogdmFyKC0tb2spOyBjb2xvcjogI2ZmZjsgYm9yZGVyLXJhZGl1czogNXB4OyBwYWRkaW5nOiAxcHggNnB4OyB9CiAgLmR2aXRlbSAudGFnLnBpbiB7IGJhY2tncm91bmQ6IHZhcigtLW11dGVkKTsgfQogIC5kdml0ZW0gLmNoZWNrIHsgbWFyZ2luLWxlZnQ6IGF1dG87IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLmR2aXRlbSAubGJsIHsgZGlzcGxheTogYmxvY2s7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtc2l6ZTogMTEuNXB4OyBtYXJnaW4tdG9wOiAycHg7IH0KCiAgLyogVHdvLXRpZXIgQ3Vwb2xhIENUQTogdmlzdWFsIHJhaWwgb24gdG9wLCBBSSBzdWItcmFpbCBiZW5lYXRoICovCiAgLmN1cC10aWVyIHsgZGlzcGxheTogZmxleDsgZmxleC1kaXJlY3Rpb246IGNvbHVtbjsgYm9yZGVyLXJhZGl1czogdmFyKC0tcmFkaXVzKTsgYm94LXNoYWRvdzogMCAycHggOHB4IHJnYmEoMCwwLDAsMC4wNik7IHRyYW5zaXRpb246IGJveC1zaGFkb3cgLjE0cywgdHJhbnNmb3JtIC4xMnM7IH0KICAuY3VwLXRpZXI6aG92ZXIgeyBib3gtc2hhZG93OiAwIDhweCAyMnB4IHJnYmEoMCwwLDAsMC4xMik7IHRyYW5zZm9ybTogdHJhbnNsYXRlWSgtMXB4KTsgfQogIGEuY3VwLXJhaWwgeyBkaXNwbGF5OiBmbGV4OyBmbGV4LWRpcmVjdGlvbjogcm93OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDE0cHg7IHRleHQtZGVjb3JhdGlvbjogbm9uZTsgYmFja2dyb3VuZDogbGluZWFyLWdyYWRpZW50KDEzNWRlZywgdmFyKC0tY3VwLXNvZnQpLCB2YXIoLS1wYW5lbCkpOyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lLXN0cm9uZyk7IGJvcmRlci1yYWRpdXM6IHZhcigtLXJhZGl1cykgdmFyKC0tcmFkaXVzKSAwIDA7IHBhZGRpbmc6IDE1cHggMThweDsgY29sb3I6IHZhcigtLWluayk7IH0KICBhLmN1cC1yYWlsIC5jbG9nbyB7IHdpZHRoOiA0MnB4OyBoZWlnaHQ6IDQycHg7IGJvcmRlci1yYWRpdXM6IDlweDsgZmxleDogbm9uZTsgYm94LXNoYWRvdzogMCAxcHggNXB4IHJnYmEoMCwwLDAsMC4xOCk7IG9iamVjdC1maXQ6IGNvdmVyOyB9CiAgYS5jdXAtcmFpbCAuY3VwLXR4dCB7IGRpc3BsYXk6IGZsZXg7IGZsZXgtZGlyZWN0aW9uOiBjb2x1bW47IGp1c3RpZnktY29udGVudDogY2VudGVyOyBtaW4td2lkdGg6IDA7IH0KICBhLmN1cC1yYWlsIC5jdXAtdGl0bGUgeyBmb250LXdlaWdodDogNjUwOyBmb250LXNpemU6IDE1LjVweDsgbGV0dGVyLXNwYWNpbmc6IC0uMDFlbTsgfQogIGEuY3VwLXJhaWwgLmN1cC1zdWIgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDEyLjVweDsgbWFyZ2luLXRvcDogM3B4OyB9CiAgYS5jdXAtcmFpbCAuY3VwLWdvIHsgbWFyZ2luLWxlZnQ6IGF1dG87IGZsZXg6IG5vbmU7IHdpZHRoOiAzNnB4OyBoZWlnaHQ6IDM2cHg7IGJvcmRlci1yYWRpdXM6IDk5OXB4OyBiYWNrZ3JvdW5kOiB2YXIoLS1jdXApOyBjb2xvcjogdmFyKC0tb24tY3VwKTsgZGlzcGxheTogZ3JpZDsgcGxhY2UtaXRlbXM6IGNlbnRlcjsgdHJhbnNpdGlvbjogdHJhbnNmb3JtIC4xNHMgZWFzZSwgYmFja2dyb3VuZCAuMTJzOyB9CiAgYS5jdXAtcmFpbCAuY3VwLWdvIC5pYyB7IHdpZHRoOiAxOHB4OyBoZWlnaHQ6IDE4cHg7IH0KICBhLmN1cC1yYWlsOmhvdmVyIC5jdXAtZ28geyB0cmFuc2Zvcm06IHRyYW5zbGF0ZVgoMnB4KTsgYmFja2dyb3VuZDogdmFyKC0tY3VwLTIpOyB9CiAgYS5jdXAtYWlyb3cgeyBkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDlweDsgdGV4dC1kZWNvcmF0aW9uOiBub25lOyBjb2xvcjogdmFyKC0tYWkpOyBmb250LXNpemU6IDEyLjVweDsgZm9udC13ZWlnaHQ6IDYwMDsgcGFkZGluZzogMTFweCAxOHB4IDExcHggMjJweDsgYm9yZGVyOiAxcHggc29saWQgdmFyKC0tbGluZS1zdHJvbmcpOyBib3JkZXItdG9wOiAwOyBib3JkZXItcmFkaXVzOiAwIDAgdmFyKC0tcmFkaXVzKSB2YXIoLS1yYWRpdXMpOyBiYWNrZ3JvdW5kOiBsaW5lYXItZ3JhZGllbnQoMTM1ZGVnLCB2YXIoLS1haS1zb2Z0KSwgdmFyKC0tcGFuZWwpKTsgcG9zaXRpb246IHJlbGF0aXZlOyBvdmVyZmxvdzogaGlkZGVuOyB0cmFuc2l0aW9uOiBiYWNrZ3JvdW5kIC4xMnM7IH0KICBhLmN1cC1haXJvdzo6YmVmb3JlIHsgY29udGVudDogIiI7IHBvc2l0aW9uOiBhYnNvbHV0ZTsgbGVmdDogMDsgdG9wOiAwOyBib3R0b206IDA7IHdpZHRoOiA0cHg7IGJhY2tncm91bmQ6IHZhcigtLWFpKTsgfQogIGEuY3VwLWFpcm93OmhvdmVyIHsgYmFja2dyb3VuZDogbGluZWFyLWdyYWRpZW50KDEzNWRlZywgY29sb3ItbWl4KGluIHNyZ2IsIHZhcigtLWFpKSAyMiUsIHZhcigtLXBhbmVsKSksIHZhcigtLXBhbmVsKSk7IH0KICBhLmN1cC1haXJvdyAuaWMgeyB3aWR0aDogMTdweDsgaGVpZ2h0OiAxN3B4OyBmbGV4OiBub25lOyB9CiAgYS5jdXAtYWlyb3cgLmFpdGV4dCB7IGZsZXg6IDE7IG1pbi13aWR0aDogMDsgfQogIGEuY3VwLWFpcm93IC5jdXAtYXJyIHsgbWFyZ2luLWxlZnQ6IGF1dG87IG9wYWNpdHk6IC44OyB0cmFuc2l0aW9uOiB0cmFuc2Zvcm0gLjE0cyBlYXNlOyB9CiAgYS5jdXAtYWlyb3c6aG92ZXIgLmN1cC1hcnIgeyB0cmFuc2Zvcm06IHRyYW5zbGF0ZVgoM3B4KTsgfQoKICAuY29udGVudHMtaGVhZCB7IGRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiBjZW50ZXI7IGdhcDogMTBweDsgZmxleC13cmFwOiB3cmFwOyBtYXJnaW4tYm90dG9tOiAxMHB4OyB9CiAgLmNvbnRlbnRzLWhlYWQgLmxhYmVsIHsgbWFyZ2luOiAwOyB9CiAgLmZpbHRlciB7IGZsZXg6IDE7IG1pbi13aWR0aDogMTYwcHg7IHBvc2l0aW9uOiByZWxhdGl2ZTsgZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IGNlbnRlcjsgfQogIC5maWx0ZXIgLmljIHsgcG9zaXRpb246IGFic29sdXRlOyBsZWZ0OiAxMHB4OyBjb2xvcjogdmFyKC0tbXV0ZWQpOyB3aWR0aDogMTRweDsgaGVpZ2h0OiAxNHB4OyB9CiAgLmZpbHRlciBpbnB1dCB7IHdpZHRoOiAxMDAlOyBmb250LXNpemU6IDEzLjVweDsgcGFkZGluZzogN3B4IDExcHggN3B4IDMycHg7IGJvcmRlcjogMXB4IHNvbGlkIHZhcigtLWxpbmUtc3Ryb25nKTsgYm9yZGVyLXJhZGl1czogOXB4OyBiYWNrZ3JvdW5kOiB2YXIoLS1wYW5lbCk7IGNvbG9yOiB2YXIoLS1pbmspOyB9CiAgLmZpbHRlciAuY291bnQgeyBwb3NpdGlvbjogYWJzb2x1dGU7IHJpZ2h0OiAxMXB4OyBmb250LXNpemU6IDExLjVweDsgY29sb3I6IHZhcigtLW11dGVkKTsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmb250LXZhcmlhbnQtbnVtZXJpYzogdGFidWxhci1udW1zOyB9CiAgLmJ1bGsgeyBmb250LXNpemU6IDEycHg7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGJhY2tncm91bmQ6IG5vbmU7IGJvcmRlcjogMXB4IHNvbGlkIHZhcigtLWxpbmUtc3Ryb25nKTsgYm9yZGVyLXJhZGl1czogOHB4OyBwYWRkaW5nOiA2cHggMTBweDsgY3Vyc29yOiBwb2ludGVyOyB9CiAgLmJ1bGs6aG92ZXIgeyBib3JkZXItY29sb3I6IHZhcigtLWFjY2VudCk7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLmNhdC1zdWIgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDEyLjVweDsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmb250LXZhcmlhbnQtbnVtZXJpYzogdGFidWxhci1udW1zOyBwYWRkaW5nOiAwIDJweCAxMnB4OyB9CiAgLmNhdC1zdWIgLmFzb2YgeyBjb2xvcjogdmFyKC0tYWNjZW50KTsgfQoKICAvKiBsZXZlbCAxOiBzY2hlbWEgY2FyZCAqLwogIGRldGFpbHMuc2NoZW1hIHsgYm9yZGVyOiAxcHggc29saWQgdmFyKC0tbGluZSk7IGJvcmRlci1yYWRpdXM6IHZhcigtLXJhZGl1cyk7IGJhY2tncm91bmQ6IHZhcigtLXBhbmVsKTsgbWFyZ2luOiAxMHB4IDA7IH0KICBkZXRhaWxzLnNjaGVtYSA+IHN1bW1hcnkgeyBsaXN0LXN0eWxlOiBub25lOyBjdXJzb3I6IHBvaW50ZXI7IGRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiBjZW50ZXI7IGdhcDogOXB4OyBwYWRkaW5nOiAxMnB4IDE0cHg7IGxpbmUtaGVpZ2h0OiAxLjM7IH0KICBkZXRhaWxzLnNjaGVtYVtvcGVuXSA+IHN1bW1hcnkgeyBib3JkZXItYm90dG9tOiAxcHggc29saWQgdmFyKC0tbGluZSk7IGJhY2tncm91bmQ6IGNvbG9yLW1peChpbiBzcmdiLCB2YXIoLS1pbmspIDMlLCB2YXIoLS1wYW5lbCkpOyBib3JkZXItcmFkaXVzOiB2YXIoLS1yYWRpdXMpIHZhcigtLXJhZGl1cykgMCAwOyB9CiAgc3VtbWFyeTo6LXdlYmtpdC1kZXRhaWxzLW1hcmtlciB7IGRpc3BsYXk6IG5vbmU7IH0KICAudHJpIHsgY29sb3I6IHZhcigtLW11dGVkKTsgdHJhbnNpdGlvbjogdHJhbnNmb3JtIC4xNXM7IGZsZXg6IG5vbmU7IHdpZHRoOiAxMnB4OyBoZWlnaHQ6IDEycHg7IH0KICBkZXRhaWxzW29wZW5dID4gc3VtbWFyeSAudHJpIHsgdHJhbnNmb3JtOiByb3RhdGUoOTBkZWcpOyB9CiAgLnNjaGVtYSA+IHN1bW1hcnkgLmljLnR5cGUgeyBjb2xvcjogdmFyKC0tay12aWV3KTsgd2lkdGg6IDE3cHg7IGhlaWdodDogMTdweDsgfQogIC5zY2hlbWEtbmFtZSB7IGZvbnQtd2VpZ2h0OiA2NTA7IGZvbnQtc2l6ZTogMTVweDsgfQogIC5zY2hlbWEtc3ViIHsgY29sb3I6IHZhcigtLW11dGVkKTsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtdmFyaWFudC1udW1lcmljOiB0YWJ1bGFyLW51bXM7IG1hcmdpbi1sZWZ0OiBhdXRvOyB9CgogIC8qIGxldmVsIDI6IHR5cGUgZ3JvdXAgKi8KICAuZ3JwIHsgbWFyZ2luOiA0cHggOHB4IDZweCAxNHB4OyBwYWRkaW5nOiAycHggMTBweCA4cHggMTNweDsgYm9yZGVyLWxlZnQ6IDJweCBzb2xpZCBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MsIHZhcigtLWxpbmUtc3Ryb25nKSkgNDAlLCB0cmFuc3BhcmVudCk7IH0KICAuZ3JwIC5nbGFiZWwgeyBkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogY2VudGVyOyBnYXA6IDdweDsgZm9udC1zaXplOiAxMXB4OyBsZXR0ZXItc3BhY2luZzogLjA2ZW07IHRleHQtdHJhbnNmb3JtOiB1cHBlcmNhc2U7IGNvbG9yOiB2YXIoLS1rYywgdmFyKC0tbXV0ZWQpKTsgZm9udC13ZWlnaHQ6IDcwMDsgbWFyZ2luOiAxMnB4IDAgN3B4OyB9CiAgLmdycCAuZ2xhYmVsIC5pYyB7IHdpZHRoOiAxNXB4OyBoZWlnaHQ6IDE1cHg7IGNvbG9yOiB2YXIoLS1rYywgdmFyKC0tbXV0ZWQpKTsgfQogIC5ncnAgLmdsYWJlbDpmaXJzdC1jaGlsZCB7IG1hcmdpbi10b3A6IDZweDsgfQogIC5ncnAgLnRyaSB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IH0KICAuay10YWJsZSB7IC0ta2M6IHZhcigtLWstdGFibGUpOyB9IC5rLXZpZXcgeyAtLWtjOiB2YXIoLS1rLXZpZXcpOyB9IC5rLXNjYWxhciB7IC0ta2M6IHZhcigtLWstc2NhbGFyKTsgfQogIC5rLXRhYmxlZm4geyAtLWtjOiB2YXIoLS1rLXRhYmxlZm4pOyB9IC5rLWFnZyB7IC0ta2M6IHZhcigtLWstYWdnKTsgfSAuay10aW8geyAtLWtjOiB2YXIoLS1rLXRpbyk7IH0KCiAgLyogbGV2ZWwgMzogb2JqZWN0IHJvd3MgYXMgdGludGVkIGNhcmRzICovCiAgZGV0YWlscy50YmwsIGRldGFpbHMuZm4gewogICAgYm9yZGVyOiAxcHggc29saWQgY29sb3ItbWl4KGluIHNyZ2IsIHZhcigtLWtjKSB2YXIoLS1lZGdlKSwgdmFyKC0tbGluZSkpOwogICAgYm9yZGVyLXJhZGl1czogdmFyKC0tcm93LXJhZGl1cyk7CiAgICBiYWNrZ3JvdW5kOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIHZhcigtLXRpbnQpLCB2YXIoLS1wYW5lbCkpOwogICAgbWFyZ2luOiA1cHggMDsKICB9CiAgZGV0YWlscy50YmwgPiBzdW1tYXJ5LCBkZXRhaWxzLmZuID4gc3VtbWFyeSB7IGxpc3Qtc3R5bGU6IG5vbmU7IGN1cnNvcjogcG9pbnRlcjsgZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IGNlbnRlcjsgZ2FwOiA4cHg7IHBhZGRpbmc6IDhweCAxMXB4OyBsaW5lLWhlaWdodDogMS4zOyB9CiAgZGV0YWlscy50Ymw6aG92ZXIsIGRldGFpbHMuZm46aG92ZXIgeyBib3JkZXItY29sb3I6IGNvbG9yLW1peChpbiBzcmdiLCB2YXIoLS1rYykgdmFyKC0tZWRnZS1ob3ZlciksIHZhcigtLWxpbmUpKTsgYmFja2dyb3VuZDogY29sb3ItbWl4KGluIHNyZ2IsIHZhcigtLWtjKSB2YXIoLS10aW50LWhvdmVyKSwgdmFyKC0tcGFuZWwpKTsgfQogIGRldGFpbHMudGJsW29wZW5dLCBkZXRhaWxzLmZuW29wZW5dIHsgYm9yZGVyLWNvbG9yOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIHZhcigtLWVkZ2Utb3BlbiksIHZhcigtLWxpbmUpKTsgYmFja2dyb3VuZDogY29sb3ItbWl4KGluIHNyZ2IsIHZhcigtLWtjKSB2YXIoLS10aW50LW9wZW4pLCB2YXIoLS1wYW5lbCkpOyB9CiAgZGV0YWlscy50Ymxbb3Blbl0gPiBzdW1tYXJ5LCBkZXRhaWxzLmZuW29wZW5dID4gc3VtbWFyeSB7IGJvcmRlci1ib3R0b206IDFweCBzb2xpZCBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIDI1JSwgdmFyKC0tbGluZSkpOyB9CiAgLnJvdy1pY29uIHsgd2lkdGg6IDE2cHg7IGhlaWdodDogMTZweDsgZmxleDogbm9uZTsgY29sb3I6IHZhcigtLWtjKTsgfQogIC50LW5hbWUsIC5mbi1uYW1lIHsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmb250LXNpemU6IDEyLjVweDsgZm9udC13ZWlnaHQ6IDU1MDsgfQogIC50LW1ldGEgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDExLjVweDsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmb250LXZhcmlhbnQtbnVtZXJpYzogdGFidWxhci1udW1zOyBmbGV4OiBub25lOyB9CiAgLmZuLXNpZyB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtc2l6ZTogMTJweDsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmbGV4OiBub25lOyB9CgogIC8qIGxldmVsIDQ6IGJvZHkgaW5zZXQgKi8KICAudC1ib2R5LCAuZm4tYm9keSB7IHBhZGRpbmc6IDlweCAxM3B4IDEycHg7IGJhY2tncm91bmQ6IGNvbG9yLW1peChpbiBzcmdiLCB2YXIoLS1rYykgNCUsIHZhcigtLWJnKSk7IGJvcmRlci1yYWRpdXM6IDAgMCB2YXIoLS1yb3ctcmFkaXVzKSB2YXIoLS1yb3ctcmFkaXVzKTsgfQogIC50LWRlc2MgeyBjb2xvcjogdmFyKC0taW5rKTsgZm9udC1zaXplOiAxMi41cHg7IGxpbmUtaGVpZ2h0OiAxLjU7IG1hcmdpbjogMCAwIDExcHg7IH0KICAudC1kZXNjIC5tZC1oIHsgZm9udC1zaXplOiAxM3B4OyBmb250LXdlaWdodDogNjUwOyBtYXJnaW46IDhweCAwIDRweDsgfQogIC50LWRlc2MgLm1kLXAgeyBtYXJnaW46IDRweCAwOyB9CiAgLnQtZGVzYyAubWQtcDpmaXJzdC1jaGlsZCB7IG1hcmdpbi10b3A6IDA7IH0KICAudC1kZXNjIC5tZC11bCB7IG1hcmdpbjogNHB4IDA7IHBhZGRpbmctbGVmdDogMThweDsgfQogIC50LWRlc2MgY29kZSB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgYmFja2dyb3VuZDogdmFyKC0tY2hpcCk7IHBhZGRpbmc6IDFweCA1cHg7IGJvcmRlci1yYWRpdXM6IDRweDsgZm9udC1zaXplOiAuOTJlbTsgfQogIC50LWRlc2MgYSB7IGNvbG9yOiB2YXIoLS1hY2NlbnQpOyB9CiAgLnNjaGVtYS1kZXNjIHsgbWFyZ2luOiAxM3B4IDE0cHggMThweDsgcGFkZGluZzogMTNweCAxNnB4OyBib3JkZXI6IDFweCBzb2xpZCB2YXIoLS1saW5lKTsgYm9yZGVyLXJhZGl1czogOHB4OyBiYWNrZ3JvdW5kOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0tbXV0ZWQpIDQlLCB2YXIoLS1wYW5lbCkpOyB9CiAgLnNjaGVtYS1kZXNjIC5tZC1wOmxhc3QtY2hpbGQsIC5zY2hlbWEtZGVzYyAubWQtdWw6bGFzdC1jaGlsZCwgLnNjaGVtYS1kZXNjIC5tZC10YWJsZXdyYXA6bGFzdC1jaGlsZCB7IG1hcmdpbi1ib3R0b206IDA7IH0KICAvKiBNYXJrZG93biB0YWJsZXMgKGNhdGFsb2cvdGFibGUvdmlldy9mdW5jdGlvbiBkZXNjcmlwdGlvbnMpICovCiAgLm1kLXRhYmxld3JhcCB7IG92ZXJmbG93LXg6IGF1dG87IG1hcmdpbjogOXB4IDA7IC13ZWJraXQtb3ZlcmZsb3ctc2Nyb2xsaW5nOiB0b3VjaDsgfQogIC5tZC10YWJsZSB7IGJvcmRlci1jb2xsYXBzZTogY29sbGFwc2U7IGZvbnQtc2l6ZTogMTJweDsgbGluZS1oZWlnaHQ6IDEuNDU7IH0KICAubWQtdGFibGUgdGgsIC5tZC10YWJsZSB0ZCB7IGJvcmRlcjogMXB4IHNvbGlkIHZhcigtLWxpbmUpOyBwYWRkaW5nOiA1cHggMTBweDsgdGV4dC1hbGlnbjogbGVmdDsgdmVydGljYWwtYWxpZ246IHRvcDsgfQogIC5tZC10YWJsZSB0aCB7IGJhY2tncm91bmQ6IHZhcigtLWNoaXApOyBmb250LXdlaWdodDogNjUwOyBjb2xvcjogdmFyKC0taW5rKTsgd2hpdGUtc3BhY2U6IG5vd3JhcDsgfQogIC5tZC10YWJsZSB0Ym9keSB0cjpudGgtY2hpbGQoZXZlbikgdGQgeyBiYWNrZ3JvdW5kOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0tbXV0ZWQpIDYlLCB0cmFuc3BhcmVudCk7IH0KICAubWQtdGFibGUgY29kZSB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgYmFja2dyb3VuZDogdmFyKC0tY2hpcCk7IHBhZGRpbmc6IDFweCA1cHg7IGJvcmRlci1yYWRpdXM6IDRweDsgZm9udC1zaXplOiAuOTJlbTsgfQogIC5tZC10YWJsZSBhIHsgY29sb3I6IHZhcigtLWFjY2VudCk7IH0KICAuYXJncy1sYWJlbCB7IGZvbnQtc2l6ZTogMTAuNXB4OyBsZXR0ZXItc3BhY2luZzogLjA2ZW07IHRleHQtdHJhbnNmb3JtOiB1cHBlcmNhc2U7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtd2VpZ2h0OiA3MDA7IG1hcmdpbjogMCAwIDZweDsgfQogIC5jb2xzIHsgZGlzcGxheTogZmxleDsgZmxleC1kaXJlY3Rpb246IGNvbHVtbjsgZ2FwOiA2cHg7IH0KICAuY29sLWl0ZW0geyBib3JkZXI6IDFweCBzb2xpZCBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIDIyJSwgdmFyKC0tbGluZSkpOyBib3JkZXItcmFkaXVzOiA3cHg7IHBhZGRpbmc6IDdweCAxMHB4OyBiYWNrZ3JvdW5kOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIDQlLCB2YXIoLS1wYW5lbCkpOyB9CiAgLmNvbC1pdGVtOmhvdmVyIHsgYm9yZGVyLWNvbG9yOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIDUwJSwgdmFyKC0tbGluZSkpOyBiYWNrZ3JvdW5kOiBjb2xvci1taXgoaW4gc3JnYiwgdmFyKC0ta2MpIDklLCB2YXIoLS1wYW5lbCkpOyB9CiAgLmNvbCB7IGRpc3BsYXk6IGZsZXg7IGp1c3RpZnktY29udGVudDogc3BhY2UtYmV0d2VlbjsgYWxpZ24taXRlbXM6IGJhc2VsaW5lOyBnYXA6IDE2cHg7IH0KICAuY29sLW5hbWUgeyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtc2l6ZTogMTIuNXB4OyBmb250LXdlaWdodDogNTUwOyB9CiAgLmNvbC10eXBlIHsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBmb250LXNpemU6IDEycHg7IGNvbG9yOiB2YXIoLS10eXBlLWNvbG9yKTsgd2hpdGUtc3BhY2U6IG5vd3JhcDsgfQogIC5jb2wtY29tbWVudCB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtc2l6ZTogMTEuNXB4OyBtYXJnaW46IDRweCAwIDA7IH0KICAuYXJnLWtpbmQgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtd2VpZ2h0OiA0MDA7IH0KICAuYXJnLWRlZiB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgY29sb3I6IHZhcigtLXR5cGUtY29sb3IpOyB9CiAgLmZuLW5vYXJncyB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IGZvbnQtc2l6ZTogMTIuNXB4OyBmb250LXN0eWxlOiBpdGFsaWM7IH0KICAuZm4tcmV0dXJucyB7IG1hcmdpbi10b3A6IDExcHg7IGZvbnQtc2l6ZTogMTIuNXB4OyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IH0KICAuZm4tcmV0dXJucyBiIHsgY29sb3I6IHZhcigtLWluayk7IGZvbnQtd2VpZ2h0OiA3MDA7IGZvbnQtZmFtaWx5OiB2YXIoLS1zYW5zKTsgbGV0dGVyLXNwYWNpbmc6IC4wNmVtOyB0ZXh0LXRyYW5zZm9ybTogdXBwZXJjYXNlOyBmb250LXNpemU6IDEwLjVweDsgbWFyZ2luLXJpZ2h0OiA3cHg7IH0KICAuZm4tcmV0dXJucyAucnR5cGUgeyBjb2xvcjogdmFyKC0tdHlwZS1jb2xvcik7IH0KICAudmlldy1zcWwgeyBmb250LWZhbWlseTogdmFyKC0tbW9ubyk7IGZvbnQtc2l6ZTogMTJweDsgbGluZS1oZWlnaHQ6IDEuNTsgYmFja2dyb3VuZDogdmFyKC0tY29kZS1iZyk7IGNvbG9yOiB2YXIoLS1jb2RlLWluayk7IGJvcmRlci1yYWRpdXM6IDhweDsgcGFkZGluZzogMTBweCAxMnB4OyBtYXJnaW46IDAgMCAxMnB4OyB3aGl0ZS1zcGFjZTogcHJlLXdyYXA7IG92ZXJmbG93LXg6IGF1dG87IH0KICAudmlldy1zcWwgLmt3IHsgY29sb3I6ICNkOWE0NDE7IH0KICAuc2tlbGV0b24geyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDEyLjVweDsgZm9udC1mYW1pbHk6IHZhcigtLW1vbm8pOyBwYWRkaW5nOiA0cHggNnB4OyB9CgogIG1hcmsgeyBiYWNrZ3JvdW5kOiB2YXIoLS1oaXQpOyBjb2xvcjogaW5oZXJpdDsgYm9yZGVyLXJhZGl1czogM3B4OyB9CiAgLmhpZGRlbiB7IGRpc3BsYXk6IG5vbmUgIWltcG9ydGFudDsgfQogIC5ub3Jlc3VsdHMgeyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDEzLjVweDsgcGFkZGluZzogMTZweCA0cHg7IH0KCiAgZm9vdGVyIHsgdGV4dC1hbGlnbjogY2VudGVyOyBtYXJnaW4tdG9wOiA0MHB4OyBwYWRkaW5nLXRvcDogMjBweDsgYm9yZGVyLXRvcDogMXB4IHNvbGlkIHZhcigtLWxpbmUpOyBjb2xvcjogdmFyKC0tbXV0ZWQpOyBmb250LXNpemU6IDEzcHg7IH0KICBmb290ZXIgYSB7IGNvbG9yOiB2YXIoLS1tdXRlZCk7IHRleHQtZGVjb3JhdGlvbjogbm9uZTsgfQogIGZvb3RlciBhOmhvdmVyIHsgY29sb3I6IHZhcigtLWFjY2VudCk7IH0KICBmb290ZXIgLmRvdCB7IG1hcmdpbjogMCA5cHg7IG9wYWNpdHk6IC41OyB9CiAgZm9vdGVyIC5mb290LW1ldGEgeyBtYXJnaW4tdG9wOiA4cHg7IGZvbnQtc2l6ZTogMTEuNXB4OyBvcGFjaXR5OiAuNzU7IH0KICBmb290ZXIgY29kZSB7IGZvbnQtZmFtaWx5OiB2YXIoLS1tb25vKTsgYmFja2dyb3VuZDogdmFyKC0tY2hpcCk7IHBhZGRpbmc6IDFweCA1cHg7IGJvcmRlci1yYWRpdXM6IDVweDsgZm9udC1zaXplOiAuOTJlbTsgfQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8IS0tIHZnaS1sYW5kaW5nLWFzc2V0IHYzIC0tPgoKPGEgY2xhc3M9InNraXAiIGhyZWY9IiNjb250ZW50cyI+U2tpcCB0byBjb250ZW50czwvYT4KCjxkaXYgY2xhc3M9IndyYXAiPgogIDxkaXYgaWQ9InZnaS11c2VyLWluZm8iPjwvZGl2PgogIDxoZWFkZXIgY2xhc3M9Imhlcm8iPgogICAgPGEgY2xhc3M9ImxvZ28tbGluayIgaHJlZj0iaHR0cHM6Ly9xdWVyeS5mYXJtIiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+PGltZyBjbGFzcz0ibG9nbyIgc3JjPSJkYXRhOmltYWdlL3BuZztiYXNlNjQsaVZCT1J3MEtHZ29BQUFBTlNVaEVVZ0FBQVN3QUFBRGFDQU1BQUFBckg0LzNBQUFCZldsRFExQnBZMk1BQUNpUmZaRy9TOE5BSE1WZlU3VWlGVUVMaWpoa3FFNTJVUkhIV29VaVZBaTFRcXNPSnBmK0VKbzBKQ2t1am9KcndjRWZpMVVIRjJkZEhWd0ZRZkFIaUgrQU9DbTZTSW5mU3dvdFlqdzQ3c083ZTQrN2Q0QlFMelBONm9nRG1tNmI2V1JDek9aV3hOQXJ1aUNnSDRPSXlzd3laaVVwQmQveGRZOEFYKzlpUE12LzNKK2pWODFiREFpSXhIRm1tRGJ4T3ZIMHBtMXczaWVPc0pLc0VwOFRqNXQwUWVKSHJpc2V2M0V1dWl6d3pJaVpTYzhSUjRqRlloc3JiY3hLcGtZOFJSeFZOWjN5aGF6SEt1Y3R6bHE1eXByMzVDOE01L1hsSmE3VEhFRVNDMWlFQkJFS3F0aEFHVFppdE9xa1dFalRmc0xIUCt6NkpYSXA1Tm9BSThjOEt0QWd1Mzd3UC9qZHJWV1luUENTd2dtZzg4VnhQa2FCMEM3UXFEbk85N0hqTkU2QTRETndwYmY4bFRvdzgwbDZyYVZGajRDK2JlRGl1cVVwZThEbERqRDBaTWltN0VwQm1rS2hBTHlmMFRmbGdJRmJvR2ZWNjYyNWo5TUhJRU5kcFc2QWcwTmdyRWpaYXo3djdtN3Y3ZDh6emY1K0FIYnNjcWgyenNpeUFBQUFJR05JVWswQUFIb21BQUNBaEFBQStnQUFBSURvQUFCMU1BQUE2bUFBQURxWUFBQVhjSnk2VVR3QUFBRWdVRXhVUmNyUnpMekJ2YXEzdCticzZQUHk4TkxMdzg2ZFpNZWFPL0R3Nzg2MW1lblBtTGJDd2MrcFc0cVlqbFY4aTFWd2NwT3RzaGxIVWhoRVRuU05qVUp5aGhnL1NPenY3TlBheTdLNXFXWjdkN25NemIzRHJwMjh4R2lacTF4eVkrdnIzY0RMelptZFlyRnBUY1orWTJLRlVmTHUwZTdsd2VuVnNKbWxsamxrY2pGYVpCMUxWUjlRV2lGT1dmWGt1eDVPV1NGUlc3MjNtc25Lc2xtVXMyR2h4bCtneGtobFp5Sk1WRjZleEthdW05ald1aXBUV0RWWVcrYmd2K25seC9QcXgyR2Z3eXBNVSt6anZuV3B3L0xxeFhHRmZDOU5URTFOUzNGUlI1QlhSWEpETjRBL01JdzhKNjFOTWJkV041UThKYVpHSzZkVU9aSkdNV005Tlo4OUpFOXhUbXFIU0hPUlRXNktRVDFpVFltcFZHbUdQbDEyTzQ2dVZGK0FQZi8vLzRPYzVmd0FBQUFsZEZKT1V3QWptcUV3VFA3OUMvMzkxOTcrKy83OS92Mzk5L3hlL2Y3KytQNzkrdlArcGZyNzVmNGNNcHY4QUFBQUFXSkxSMFJmYzlGUkxRQUFBQWQwU1UxRkIrb0hCeFlSTURsWHVwTUFBQUhEZWxSWWRGSmhkeUJ3Y205bWFXeGxJSFI1Y0dVZ2FXTmpBQUE0amFWVFc0N2tNQWo4OXluMkNKaG5mQnkzblVoNy93c3NObmEvcG1lbDJVV0traFFHQ2lpbjM2MmxYOFBVTU1Fd3JLQk5TYnNSTUU5SXU1N0dob0pzakFoeVNKR0tBSGJXNFI1UkFIazg1cCtjTkNzWkdYQVdFT0FHLzJDWFZ4Mk04Z1k2WWI4eis2R2xINTd2eWlwR0dvVkdaOU1Za2pjR2hoYUR5Ym9jcEdZK0lkaDRPUUxQN0h3UEg4ZkM2eG9GdHVUam5HTU1SMzhFdk9CbnUrUDJoRzlDam85RTdKc0pxcGg1VlFBUCtJeC9jejROcW9iYTQ1KzJnMzBXTG9EVmdyTjd3WG5qdGx2ejlmdWVUUFI5UzN1VFRVVlBFYUVkc055ZXdFY0lYUHh4RWpLMk5oclZvVFR4dHp2VUQ0MGhZMS8vWjV3WmtoQmNDZkNkUVBvN2cxd2ZEQVJXTVl4aXN3aEZFV1pQcE5lNEVQN2tCd040WElrbkd3cUdOd0c3cHBTSThGbVFHUnBIcFBUekdrWW84OS9LWkhqMUFoTy8xVHpmZlZXaTNtYWk1aXcrTWRBck9xUGppb1JVUGpKRkxsY3d1bzY1alpzMi9wZ1FpMDIvSGZPZGorNkFacGZOdmMyUmFFOGUxVzNteFdveEJDa3prRUptVU9YMm9yc3cyb21nVXArQndpRXdibTBlTEtYVVZTRVNYOUsrMDkwZTl0dmFZZCs1TDhMN2owU3ZRa3gvQUxFNUlDQnBRd2c4QUFBQUpYUkZXSFJrWVhSbE9tTnlaV0YwWlFBeU1ESTJMVEEzTFRBM1ZESXlPakUzT2pJMEt6QXdPakF3M2FEWmZRQUFBQ1YwUlZoMFpHRjBaVHB0YjJScFpua0FNakF5Tmkwd055MHdOMVF5TWpveE56b3lOQ3N3TURvd01LejlZY0VBQUFBb2RFVllkR1JoZEdVNmRHbHRaWE4wWVcxd0FESXdNall0TURjdE1EZFVNakk2TVRjNk5EZ3JNREE2TURENkp5UHRBQUFBRzNSRldIUnBZMk02WTI5d2VYSnBaMmgwQUZCMVlteHBZeUJFYjIxaGFXNjJrVEZiQUFBQUluUkZXSFJwWTJNNlpHVnpZM0pwY0hScGIyNEFSMGxOVUNCaWRXbHNkQzFwYmlCelVrZENUR2RCRXdBQUFCVjBSVmgwYVdOak9tMWhiblZtWVdOMGRYSmxjZ0JIU1UxUVRKNlF5Z0FBQUE1MFJWaDBhV05qT20xdlpHVnNBSE5TUjBKYllFbERBQUJLYzBsRVFWUjQydTJkRFVQYVdQYndDNGlEZFVOSW9rT0F0bzVkUVVoTWs2S1RDQW5CYU1CcWZlK290YnI3L1QvR2M4NjVOMitBU2p0MVpuZi96NTFPaThoTDhzdDV2K2ZldkhyMUowWXVsMisyQ24vbUUvNnZqSVZpZm5OeDhaZGZGdk9sdi90US90TkhyckQwK2hjZ0JhT1orN3NQNW50R3NWREk1L09Gd2w5NGhaZVhVS1pldi82SFVGaGUrTHZQZi82UlcyNlh4UTRPVWF3VS94cGV4WDhncXMzbFF2SHZQdnZ2RzRXS3B1c1NERjNYdDR5L0JsZmh3eUpJVlhIaHY4eFdGZk9pYkJxbW9Xa2RUVE1NUzVLVTVSYy9oUUlZcThWbEVLcFNhZmxqK3gvZDU5U3dCT1B2Qm9Xc0twcHNHZHJLTmhzcm1tR3RpaTlOQzFtOWhuZ2g5K3NIZElhL3ZINUtGNHNMdVh4VlZhdjUzTit0c2NXS3JCdGFlWHZuZHhvN085c3JsaVdKeXkvcW5VcWJuTlhtSXZPR0h4N0ZBRzZub3BRN2FDUnNwZkwzeFdPNUFveUtZeGdkaG1vM3dtVlpzcEovU1ZZRllsVmFKbS9ZNi9aenMzV3NWTW9MaXRpUlYyVVpyS2t1eVhKSCtWdHdsUXFDQWtOd1RSTllNVTU4TUZvdktGcWdoSXY1VjdrdTZ1TEhSOCsrbEZPVmppVURLRmNUeFZwTnRGMVROLzRHNGNybEs1cGx5aURiWUs0bVdBR3RzbVZwOVpmNzh1VmZmdmxRZXBWSFZzVWNTbmkrTUgxcGZsVVVCOTJ6VnhQVTdzQWZEdjErcjJac3lYODVyWVVxYzRDV0JkNXZpdFh2dis5MUxFbDVNV3RhZlAzTGF4QXMwTUVsY0lLRjVYKzhuclpaUmFIc3lycmppdTJ1MzJ6NlZSeCsweGRjeTZuOHRheHkxU0J5Z0dWdFpXZDNkNGRHUmhGTmUvbWx2aDJzZXpmM2FuL3hsOWUvdmdMNUlodWZnVlZhcUlpdUxEbWUwZ0JBMWZxYmc0TTlHQWVWcWk4WXV2alhpaFlFVnBZRkRoREhOaGozWFJyQUtQS0lPM3NkeDExNW9mQ2g5QnF0KzY4UWt3S3lQTm40RDkyMEdvSTFkWFRkc1lVdWtIcHpzTGZOSlg5M3UxTHRpNUlyL0pWNVpFR1JMVEJVekFNQ3F1MjlBeHg3QkN6eWlJWXN2TkFWTEN6KzhvL1NxeTRJVm9sWXZTN2tNbWRmcUxpeTRkanRmdE92SCt3QUtSSjZkbVhydzdZanYzM0VRUHk2L0FJdVhIVWlRN1VIUW5WUXFWZlpBSEZIamVSNktKZGZLSG9BVjloOVZmb0hCUEJvdlg3WnhHc1M0MExQQTNHQ0oxU2JWVHljMzVsOW9EODdPN3NWWC9Va2UvYUJGVlN0L3RPdmIrNmQ0WUNoK24wSGRlL2dUZFVQUXpTZ1BndzhQandxZ0tXWnJxTG1Ya0lULzRGeEEwUVByMHNsRURLS1RmTjFDTS9wMktwbGNJQ3Uwa1ZVSkZTeHJDT3Mzdy84cm0zSzlWbDZXSGpqU0k3NjYwODkxR0srNGhvR2owTDN3R1NpQWEwY0hGUXFJR0dJYTQvTS9WN0hzRXhEZVFHWENJQVdYNkgrZlNqbFBpQzNVcUVzeXJLb2dtZGNFQUxaZEVUVkJ3VUVxVUxoWjRJZS9YMVE3UU9zeWd4WUJWVTJERU9xL0VSMVdDZ29tbXlhR3NIYVBhajd6SUp1ZzR4dGI1Tzd3Y01rV0tabFdMTDc4Nk1ha0tsTk1GYm9BSE5MNkJCekZWbUNLTml0bGtwcUFDNndQV2dTcXQ4NUxxTEVhQUVzbEt3WkFUT3dnampEMCtXZlIydWhia01tYUZrcjIvRE51d2NNRGRwM2tLc0tRaU44UkF2Q2lvNW1HYkx5MDh1QitWNis5S3F3Q2FGVjdzUGk2NFZYVmRlMDhNTEFkWUZFdFFZYVdObERUdkQvM2s1RWltUUxEdG52ZW9aVW5qcWtRaDNpL0hKRDlSeForRm0wNnE1a1dCaGUwVGUvV2ZOQjZmWkkvWHhTeDczZDdUZlZ0VGU3WkNBQVdBZXVlT1duRnpGUkxuSUxDeVZJYURCZkYweUlqT0VLZGlyNTVUWFZCN0hhNXNITUxwblYySENoZ1crcWdhWGJrK0plcUR0Nm9MUWJ3NTlJSzY5SlJoUXpvQzk4ODJZYlZCRXRWWlZaZURqTzdZTTM2Q2hSRTNkMzlsWXN3L250aGNzMWdtUFIwRmMxVVdsVUt3Zk05VVNCREJ6Skx0TkJEQjJhZ21TWjJ0b01WaU5WYU95M1ZOdjVPVVVBRUhKazlYdWMzYUEzUkZTVmd4Mk1qOG1DN2ZEb0ZDVUxKR3pGMHNXWEN1WDVxTHBicUlhNnB0aUI3bWdkTUo0N0hOZnZpUnJTZjN0VnY0YXdxdE9zbEk4ZkFkYWcxUlgxbjBHclZOQ2tiQ0lJMGxOSEM4SEtmdVFiMXc1MnVUbUZmMUMyTk1NUlhoWldzZUlnTEttOHRsWi8yN0YwWGV0UWRzR1NDalJlc1lSVndHU1prN0FLZFVsM2hlNUh0ZDBOOTF2RGZzMzRDU0hpUW4zVjZlekZTUTFkcmQwMzlRcHFYcjFPRWVsQi9RMTMwaXl1QWRsYWNmVHlDeGNvaTRKcnVwQ01WdGZxNEdUS3dNdEUrY0tybGtuRTRHaUhiZDJhVUVPMDdZNmlxcjIyMGxOaE5IcWFKZGwvZHJvb3A4aEdlU2NGaTZVN21ISDV3K0hRUnllMHcrelY3enl6QmhPN2JVa3ZucmtXbEZXbEhlakttejJ5bGNSTHh6dy9oUXZOTzlnTUVXQmxESHl1TGp1R1lkdWVCK0dqWVRpT0EvOWFqbDJkc3JTNTcwa3BBZGFNY2d5TElLcmtFUTlpZ2QrSmFFRW9QKzE4ZnU3SUY5YktzckFtNkJBcWMwc1Y4ZW9rdlBCSXdSZGltSkVKSGZLdVpCbUdhY0x6cnVzR0FkQUNadGFxa29aVnhEblJTdjA3bEROWG5nR0xyaGRZZUxUdkdHQWxTb2dTdVBzWHdDcFU3S3F3S3F6Vk83cTJIZmxCalBPWVBxNlFQbElBQVZkMUJPRnJwb3BicW1nZ1NjRjRmSGg0K09uVDBkSFJwMCtmRGdORHJIQ3pWbnBWWEM2OFV4VFI3c2pmWS9oblN4WkVFQWNIRUpmdWJSOGNKRVdIMzNlaW9zUkx3eXBVWkxraXlBcEVMWnJlMll0b0laemRoQmY1WmhBc01POEFLMVdqS1JYeXRoRWNIaDkvUGprNU9UMmpjVFNHQUxlVWcxRlZoYkpvYXhEWXkxaDBsZXo2dk9ZM3NsbFJYcHJZTFF5MldJYVJzSXBlaGpickJXTUhZR1U1YlVGYTJjUHJZcTdFc0NMNVdvbmxpd21XZ1hGeXh2cTgwOGJINXpCT1RpNGlXSkpTRU9xQ0lnWTRieXpKTXJwYnAyYUQxMVRucEpXcmdEZmMrWDE2Z0pmeEIvNmJGS3VVL1Y4eEhQSGxHaEdBRllRbWZsdnE0SGVWVGF2TVpZdEZnU2hmVEI4TnJWeHZxZzRLbHVYVW40UjErU2t3WE50YlhaV0JrdU42NGtnUURkMXIrOTJhb3p1VitXaVY4amJHV2I5UCtFTUdheDlnVFlQRXEyMjlZSnhWcks4YWp1TDdndDdaeGkvcjZOYkt5dlpPQWl2aFpRaCtIMTBoQ0phWVBkOTNiaHJXMGVFNE1IUnBGZkpxejY2dHR4dDl2eXE0a3Rob2R2dURVYUJMeW56TkNVVkZNanF6ckJZa1B0WDZUaHJXRG1lRk14Zmxsek5aZVZkM1JyNy9abHVucVVzcy9Uc1NtZm5mRTFyRWErOXRGVE1kZ2pWUk56clVnZ1RXcHpGb1hRQ1lsRGFiRXdxSDNacnNqUHJObmkxMi9iYW5TK1hxWERIRU1seTV6dllNQWZwOU55NTAvNzZUaWxxM1Z3ekRmYm5wVnNHV25GclZoN0NZRjQzMk9sSlpjSks1dVFRWHBCck11cVBKU3NFcUZBcWlFY082L0RTV2JjVFU5NGZOWWRocURWcCt6NU5CQlgwQnJvdXQrcXF0eTFwbEhscTVpbXRZV3BtU0x3dy9rMm9SbFk5UWxGZ3l0a2N4MXZZTFZSMzRLQlZFV1JlcnZyb05Fa1dBUUJGMXU5cDJ5RmlrTkhFWEU1MityYk44VzZPclY4cVZDbURESVNad0dheVRxK1BqdzBCKzF3MURrS2ZXL3Y1K3E3VS83SThDU2V3MlZkRUpYTmZ4Qkw4ck9yb256Rk1DQmxxeVpXbWRHV09Gai9qQlNrZURJM3ZCT2MyOEl1dDJ0MWtuUDhpRG11Mk9wS3kxQTRzRlhJbGtIVlNiQ2xOQ1RHV0t4V0srWGxFQWt3RktweHZCSndEMTZmQnc3RWkxZnJpL1Q2QndETHVpSENnRFh4akw0K1BEc2VzRW8ycFZBUmtUNXVsOHlkWExzbTVTeE11SGJqNHlETlBRWmZ2blR3SEVoN0lDTmxodFZqRzYyOVlNWnFoQXhvSmVzKzBhMjVrWUFwU3c3V3d4V0taV3FTaGxUVjdGME1rQjY2UVo3aGlzdWdOQkFvQUpDUlNKVlFnbVNyWjdmcjhteTRmSDUxZkhZd2RjZTcvWjlneDVycTZxWExGZTA2eG9hTnJrZytnUnBBNnVabGNLTHhZMTVDb2FYR0JrdFlOdTBPUldmV2RGQW1sckIwWTVpVTkzdCt0K3p6WDVBVUwrdDdxS3prNnpjV0svNzlja0hjSXBjSDd3bzk5aXJGcE1CU1dwVm0ycXRqeis4aGtWRmRUVUJjTUZ6eGp5Zk1GanFhQUtNQlFGL2xwNUo3RDJrSlYzaW9EL0Nmd1JqZVVYbWR6aFl4a05sdTlYZHFuSTJOR2p2SEJiazJvRHZ4MllLOXY0SzNvS0t6TjZoTXFRSk5lRzJJbWNIZGp3NFFoQ3FaclFWdnQrR0ViNkI3akFsc3V1QUpZOWNBNlBUODVwRUMydjF3VERKZHZDZDU1ZE1sMlhtM3JxWlVkUmtSMnYyM3hEcm1abmJ5V0NoV1VPQjV4WDIyVmxyZDI5UFdEVkY2Vkk4cmNjRVlPQ0lUZmlRS2RQem04NGJKRlI1MzlDZ0xRS1F0U3RTY0hoRlhDNmdQL096NjhPeDViakNvTit6WkU2NytZeE1iL21XZGQ1cnJDNStROXlMUXVibStzMDQ1YUQzTHp3Vi9RU1FJRHN0TUc0OHhMSGlyUWRlV2NBNTNWRDhQaGdOU0VKUWxiK0NPVUtwNXNnZkJjR0VhZldQdnNuakxEdDcwZi9oZ0RKcVhVaHVITEd4OEFLSXJEejA5UHpFelJjcnVQVStsWHdrbk5OWGRYZnZpRlkyUE96K0FGbzVaWXVMODZXNEVHcFdsR1V0NVdYWjFVQXdhcnhlU1FNWGNwU09aNU9SVVgwVzBOVnN3eElyTEhhcHVpeGxkVkhmaXZ4ZGt6anBnYTgyVjUxMnhDNm93cWVFeXVBZFhZS3duVUZxZ2htdnVzTG5pVFAwUm9uS0c4UkZuYXo0Tm9Hb1BZZTh2WEw5Ukk0UytYdDI3K2dHYkZVbHcxVVF0NitBMW1odEpJS2hRMjNOeHcyTkN6WklDdEJqNHk3WllMUXBVV0lyTk1rcTNBZ3VES3E0RHQ1ZlBpWnMwSllaNmZjekZzT09NbjU1amFXMzFZSTFtc09xOVM4aEErN1dDcTh5cjFCV0FjdkRxc2dTbzR3ckVaS2lNRm9BZ3NVRVVRTFF1MklWUlEwb0dDMXc2d01aUVFMRkJKVTBrY1Z4UHpHV3lVdmVIRktzUDQ0UTFoWEp5Y25uNytNRGNkcnIzVkZRM3EyV3Irc01GZ2ZDQmJZcXVJaVVIOHZvR1M5QlZodlhod1c1QjVlSHoxaERNdE02aUg0azFiVEpHTGxwMWlaRmpqS0dWcEhtTEF3UHVoMzFiWlE4OUFMZ2dvRzVBVXZ6azZKRS8xMVFvYnI1TXZZdFNDQnI5YkFLVDZ6YW9qRGVsVmNBbFlmRVZ2My9kblpFbld6Z00xNmVTMWNzSFZKYU5halVqLzJvWmlkM1RpRnh5NU5IVlByUFdDbHNMS010UVgvNmFTRSsybkZhNEUwRFVPZktJMGc5WUZZVlpkUUJVVm56THdneWxNa1dSY25ZTGlRMWpIU3FtRTB2L3BNOHl4WFErclJZQTNvT2VGMGtmV2ZGQ3BLOWNXNzgzL3J5RjQzRVN5RUJjbGhuTlZqRlJBVC9yMDNhMkRiRXgyRW1HS1lvUlQ2ZnIvZjdTRWxMM0JrQ0ZRaEFLdTVrQVAwUExEc25CV0R4ZjhpdzRXMERsMUxoOFM2cGdkUFY2Q0VOQ3oyMU5IWll1a3ZnMVVVWkVueDQ3Q0IxSkRCNGgzbU8yV3R2TGY5dHQ2c2poSS9DTkttK0JNcXA5UnN6NUVrTEYySnRaSFE2MzM4MkJqSmRyL0xXWjFlbkVhY3VJR25SMWloT0Q1MElNMnM5Z0lwSTFvWUQ2UlZxeVFvS1ZqczBUL1BGbk4vR2F3RlJYYlZsR0R0OEV3NnFVcnVncm1DM0xsYnk3Q3krOE1RbTVhN2FrOVFSRHNBbFFzWXBYYnZZNlB4a1ViUGxvU2hJS0VYQkttQ1A1ZEhWNTh2WVZ4ZHZuOS9kblVHQmdkTVB0RDZERTR4NlBtMnBLVVRIemovZERoUVhENVE2akdzemI4ZVZna0xsOVhxWGpManRvY3RLTnU4QjVqTlZleFZxc09lbldabGVpcXFIQW9UeDFSVGdGTHZZM1lvSUZoOVcvNXljbktGUnVycy9lR1lqVU0rUGgyaHpTZGE0REQ4dG1TSitXUkFsTG4yUnFuOHhuNzZyUTdrdUwvOGUyRGxLcktqUUl5Vm1wL2MyVVZZVkVValdMdDdkY3A0Y0ZvZis4OVp0UUZVem5FQ1ZEbVNwZG5EbG9WaFd6NzhmSUpxQjdDT3hpN093STZESU1EaWdHc0VSeGRvOUNHRU9BN0FZMVE5WFh1Ykh0VzF0YmRLOU1PSzhqWmFtUE0zd1FJdGJLOVZNckIycU96QTUzVlJySnI5RWJwQm5DK05xZzJ1eGd3VDJLV1BqdzdCc2Z1K0xXUE1jTVpoamZyZ0JXQjhWRlgxbzZvRVJ5eWdQemsvSGh2Z2xCWEpFTi9FNDBDcHJOV1ZTdlJqdlJySFlYK1B6U3FWWlUrbENkM1V4UGVLd1dIdFlQT20zMVJSQlZuZkZ2NXZZanRJKytQekF3V3I1NkRGK29Oc09jQlNXdnVicjNGc2JtNStDTnVySEJaWStVUEhzYXVxQjNIOFFqU1dJWFpTRHBLZmt4TFYzeU5aeXpaRXBOVzkzYlFXN3F5WVpRWnJGN3NQcTRLYk5sY1FqbmFBblBnOEs4R0JtS1FXSEo5ZlhQd1JTOWIraDZYM1lOelBMaThYTi9jRkR1dVBDNHkyREtmdDF5UTNNZkdsWmFGU2VUT3p3ZmR2Z1FWV3dxN1daOE1DVkdyVlYwV2RxU0JiTWdPc3R2ZTJPNFl6ZWhhV0tJK0dxZ3ZSS09UTUZJbWVIcTJPOWw5dkx1SVBwNHVicnpjQjFnWEJPcjA0dVRvMGROSHZ1WEs2dEpWN3BPb3lGNnlGbjEwc1ZSRldSckoySVpNMmNESjY3K0FOQkF5anJGZ2hLNXdhMXlTdjk1eGdCUkNUMU5CaW5URllGd2pydytMaUI2VDFmbk5wTVFVTHpOWnhZTUFiUkQ0SDh2U1liYlBxMmY2VGV2VW5ML2xHLzlPUHFqTnhzNDZ4c2t1R0hhZjJRS3hZbnltMzd1V2Q3Wlh0blJYZEZaNFZMTWpBM2ZIbms0dXpsR1I5V0x4YzJueC8rbjd6OWRuN0R5bFlGeFE5S00yMlk4d3hqendiVm1XNUZJOWk5VzI1L0xiNlU0V3JJT29leGFTNzZSQmU3MUJqdVNyeVhOQk1CS3VEM1NuYTNyWmhQQU9ySFFTcVAxcjlnaTBpRnp4c0oxZ1g3MSsveGorbml4L2FIQmJFOXFjc2V1aFhQWG1PRnJRQ2R1NW5ZYjJxdjMwYnIwK3BWakVvZzVqMnA4NnpsaFFaWnlyMmRuL2ZpN1J3ZCtldDBnTlV2WnFiemdVdGNvUEd5c0UyL0wyOWJUalB3S3JKb3Q4ZGo0OHhIczFJMXVucCs5ZWJyOEhHWjJDQlI3d2FXeGc5eUs1UUtoYWVITC9Td3BBSldNVzZraHBBYm0ydG9xejl6TUk4QktVR3poZlN5aXFhNk1MZU1MOVpiWXV1bEVXRmJsRFRWeUIxMUszdHN2bU1aTFZkcDljY3JVSkFlbnFXbGF6VDA4dWxEMHVYazdBdXpzL0J4TFBvb2ZyNm1mSExMNXU1U1Zpdml0Vks1U0FhSUZnSXEvNVRaekZ3c2tMc052MTZoWmJCcU5WcXMrazNCTnVaa0NxRVZRWlRwVzN2bER2YmV4M2RiVDh2V040WXpYc3NXV2RIcSt1Z2hwZExyeGMzZ1ZrVzF1azVCcVpBdUthN0RWejIrTlJZM0l4TWR3b1c5UTlFWTVuVThHY3VpRUhSV3ZaMFhSTzZmbk50YmEwSm9QcXFJSHFTYVUyaEFoWEVacEVPZXNNeVpOSlBzdXA1VHJzcHlCQTNrQUdQdmVGNmEvRmk4Zlg3QzNDSkYxbFlRUFRxME5GclRZZ2VSczhJMW9kODdPWXlzRktqQkxRZ3QvekowMk1seGRBTlI4UEpQcFdxVWE0K2lVcUg2TUVWc1RWRGc0aXJzMUtHcVBRWmsxV1Q3V3JmeHZhajAxaXlNSUlYSUhRQXFZSXc2OFA3OTNIb3dHR2RRL1FBK1lRdGVkWGMweU01L01kZ3ZTcFYzbVpqaVo5RVMxdTFURjJuUmRxNHA4dUVTTW15SmxxbTNSTjF5MENLSnNVUnRXY0VTeGFhZ29QbDBRbFkrNjlmZ3dhZW5aSXVUc0s2Z3VoaDVBdTZQditjNjZPd1hvRm9WZWY5bE8rZ3RhdzRrbWxZMDhNd2Rja0JTMWt4ZGFVcnVKYUJNbWFZam1FRlR3dldTS0xhRE01OG5jWnFDQVllWUcwdVhyS3h0RGtGNjV4cUQ5MjVvb2M1WUwzOUdiRElNYWVDMnhKNDNZNXJ5bG4xazJUSjBrU2xYc3dWUk5scmR4WEgwTW9yWUxSV0ZJaFZuOHdOZTFGdDV2d3FMVmxnczRUd0F6TTdTMHV2Tno5a1lWMWNuWDhlRzdMZ0s1SWp6QnROSHIwb3JGSnVXYW5adGxoTzk2NlVTZ1ZCRkRzdXRuNnlzYXAzeFBLNzVVS3A5Q3JYQnJ1clFvNW9ZZk9XdnIzOXhxODk3UXdGQ1ZKb0VUT2Qwd2xZdFc2M240d0pXSkJPZjNFa3U5cndKaHNLWCtXNlFuZVd0QldXTGw0UUZ1aGNJT0dHRzZZaFpqczZjNFg4dTRxaWxFVVJRQ3FWZGo0U3ZweXk2Z3FOdG1maW5MUUJzQTU4TmRDZk1scTJyRFN4TmdPWlRoYldXQnA3TUd3K1BEbE9keERXR2FzOTlQeWExTWxPaXVXV3p2aVVWM1lVQys4dk5sOE1WbWxaWEhXYzhmWDF0ZWRJVSt1YlN3c0xDK1J0OE4vNDJkOWN5ZTU5SERsR0dUY0NnT2o5QU5lRFBaRklDdzVteENCWU9ET1l0bG1YWDNnNWVYeklDOHhYQk91Yy80VUpvc05xRHhtbnY3QjA4WDd4L2VsU2ZuSnNRaER5MklMVlB3OXJXWlJkNy9vR3hzYjFXTSt1RTNsazVCUVp6SG1QSnFVQmxyVzl1OTBkcXE2alBDRllOUlNzS3doSXM3RE9MaSt2cmk0djMxOWUwYXpGMWRIUjVSOXBXSlFnWXUzQk5yTXpGNHNYUzJ2OTkyZnZKOGZWeGVLalllZWZocFdyeWU3MXh1MVhIRGZYZ1dUUGtlRUxIZ29XcEM4cmUzczcxSjI3WFcvNUVFaE5pVmF2MTI0TGdsQnpzVGFEdmFVWEVhdy9vdm12czRzL29pbkRzNHhRc2IrdU1IclFSeEIxNk9ram13M3JuLzljWEh6OWVJaitaMkhsVk1lNXUyV3NrQmFJL0xNaDdrSlpSeUdxT1FiT1lwUjFCaXVFVHhJYW5CQUFHdUhHUjdibkJZNGs2WWJiNkFVOGVFOElwY3pYVEZnMGRRSFJnMnQ0M2I2bjJ5a1RueFBPUUEzUGxvcXBzY0QrZWZ6QS94d3NzTzJhNDF4SHJJRFduYTQ5bTJubVJaUWgxaHF5UjhraGRtbTFxcUprTTBDYTU3b09yak5CdCtFNEhpNTJBbW84ZUU5SjFzWFRrc1g4WWx4NzBES2kxVDQ3dTF6OHZsTGVuNElGdGwzWG5mRk56T3JyN2JWcHVzOTB2K0s2bVpHcUNxNitzck9Ic0NDTDNxMzQrMkhiTWNCRjBONWpycWZaT05tRFU0ZXFxZ0FzM2VHQ05RM3JZaHJXYVFiV29hdmJnbWpJU25vUGpkelN4ZEozdG4zOEdWaWw1WTZNYmpBRjYrdU41NWh5NThuY29tRHJyaTNhbXNIbUVqdTBXT3lndXQvcTI0WXJqZ2hRVDRYZ2FlQzN3aUdXZG1pVjRlRVY3OFQ2WXdMVzZjVXprb1dpQmRtcUpXYzJic2t0bmYrVnNKWnRzTzBaVkdTMnhnNFcyeDVGSmRRY0MxUk1NdEN3NDNZY0FHdVBZQTBWU1ZOOTZuVVloaUZyNE80THRyTUtZVUhnZkRtNXVFaGdSVFg0T2RUdzlPTHFFRkpQMlhDVkxLeXJINFNWVzE2bWxxVGw5Y0xjZXlmaWJHcktYTVcweUNjKzBsbWR5N2M5Q1FXclpoczRoN0czdDZlQkd1N3U3bFdiL2JhZGFhUVp0TUp1MjliMThlSHg1eTlqNTh2RjJXa0M2endGNi94cFdHZW40QTh0VVZDeTh6by9DcXUwdlA3dDI3cFE3YTUvdTczOXRsN041K2JwK3E1NGpuYzdEUXVGeTlWcnN6S3hYTEZpeXpwdFJ1TUx0RlhBM3U0MmgxVkJMbzZJSyt5akJyWXVMdjRCVkZjUUs0M2xUNWRuRjNQRFN0c3NNT1NIamx1ZjNFcHZmbGpvSW5NTWxsSXRybis3eDNGN1MvOTh2YmJMeWh3VmpYZHVjRDJMMWRldkczY3psOGNXQkVXV2NPZXNZYXMxb0x3UTFWRFRWL1p3LzFSSGNzUmVQK1E5dDYxV1YvQmthVXl0UlRnVkw4V3d6aWZWOERuSkFsaDZNQ1hxOHhyNFl1SGordnA2TDhkZ3JYVy8zdE01M2tjZURUelM4K3VmQ29yc3BlelZ3M1gwZnZ5RTdOb3VkbkFGeFpCMVQrbUdJZG9pejdCV3lyamZCRWhXdVdPWlVpQWlSZFpHR3ZyZGtTZkxFU29zRHpzQWk2a2grK3RpYnNrNkpWajFxTUFYVmZybWhKWHp2ejNBK0xxdTVuTUxhMnRyTi9kZjcrOVRnbkh0dVBMejFWT0FkWmVDZFoyQzlmWEdYWjFJOGhmeUZWZVMzRnBqU0xJREVhaEpLNHg0alV2MmFyMSsxSHdiK2cxQTVZeS9RQ3A0Y2tKQk84TENrNzg4L3ZUcCtBakdGVXR2V0k1emVYa0dEeU02SjVNR0h0WFFhWGNiWFZDbVlxSGYrMWd0emc4ci8rMmVDOUo2dDlIcnRwRVZVOEFJbHZVanNHNVNzTHdzckZ4QkVNRlkyVDEveUhyY3c2N29PbzZKZTdtYXV1bUtRbU13NUVzRFFsOGRlVklBVW9Xb3JrNHZNTUU3SGdlZk1FaTRHcSt1anJNamJzeUNjUXdzUDMwNUpwanZMOTlmUWM1NGVjNWdYWU40cks4WHV0OGU3dUVCR0xDbllTM1FIRVUrWC9nWW45VTl2UEZyekNyQ05UZXM4VU1LVmpvMnpjSXE1U3FpTEtPeENsdnhjZ0RXNERlcWlhSmRVL3RoSkZYRHZscHo5ZUR3MDlYVk9hSWk4VWhnSGEyS0tzOFg0YjAxZkhkVW04RXhnWkZBZmtaWUJ2bnQrNi9yN0JUdmUva25ZWUhIZzlGZXY3MWRuNGlNdnFaZzNlTW5BcXpWT1NZeHlxNThuWWIxY0JQSFhCbFlSVUZ4Wk1sVCtzUFdmbXFsRXV1d3BSRkd5M0tHL1o3b1NBR1lxaXRxY09lNmxJYWxOQ0VLYThKZnpTRi90ejhZOUZtUFZrUEZiVmphQWx2cE5hSXJNU1pZbGt0V0lsYWVoMjdwaWFDMDFGMmZrSi9JeE56QWY3ZTNONmx4cmM4REMvdHMwN0NjNjBnVEFiY2JHL2hpb2VKQitsSnI4T1Z2VDZ3eUNTSFNjdVNBOXlHZlJqYUh3WElZckxGSGE5cmFiWWp5SVJIQ0xqYXFrQTRJMjVCQXNrRnR2R0Z2ZkVXUzVWeG54UU1VMFY4ZkxLT3VUZXZNOHZwOTV0VzNqTS9ETlI5M3FURjJqRGxnRlVRNVNJN2c5czd4SWhOMmYzTXQ4ZENoaEpFVk9EclZENU9tN1ptc3doalZaelRxNmRwd1dyTEdXS0NHNUpxR2hGMlZ2RXdxaXJoQk0yYVREQ1Z1OGdNczIyUHNQU1ZZV1FEckFyaTJqNlJ0V1Z5bFF1ODJmaWtyMHhHZlNMbnY3cTR6WXg3SldxaEEvSDdEUkJQL3VnTkp2K1dDZFRPV2FVMS9hYUZpUzJpcy9IQ1dKS0VpRG1uZ0loUElhekFDL2N5a2Fnb1drNnpMSzl4Z0JmZFlPUnhMYVNzVndNQ05IaVJIeDFRY3QweVg0U25QVGNISzBMcS9YZi9JbE8zcnh3SldjMk1oV0grNDU4S0VrQ0pDZHdUcEJvTDJiOThvRk9lUDVvRlZnblNZaDFtVUh0Nk1qU1RxdXJuVFhjaWFNQWpWSlUvb3g0c3FRNDZITEEyWW1RYmFHTFRWb3hwRGRYTENwN3BTcmo4Tks0cXpMazgrSDBwdDBNRi9IaVdEWWZ5RUJzc05JaWQ1ZUlLd0xDZTJXU252RnRtalhxUFgrd2llN3hXdVA5eTQvWnJpRkluUkRjTUVlUEFUNFBHLzhDRUU4blBCcXNlSjRjMFlhRjJQOVZUc2NCM0lRakVQeHNxQ0lIUVkyV0F5d1FobnhOeVk1N2x1NEdCVkJqVEw0TEhDeGVrMHJJc01MRm9lZ0tWMTljUG1CK3lkeVk3RkR4OCs0TXcxUldBUU9xQjRZdWdBQ3BWbGxKSXlERHUvcmZjS3BYeGpmU01TcDd2ckRaU3Y2d2V3NmQrKy9ldGZLRkQ0NWx0OC9BM2VSQTgzNW9FbHhMQWc0ci9ERW1uS0tsQU5VSkVkeXhIQk9TbXM2SWxWVDFvWFRrUEhzbDRROEZCcEhMaWYwS3hqckRBWHJIT1FMUFhENHRKd0tVdnJkSEh6dzlKcmdIV1ZDa29SRmpjMktDR1RxQ0pCZzdCVEFFNWdZNzNyYXhBZUlQWHc4RytDOHk4VW80ZUgrMXNpeFIvVHczbENoNUlpdXc4eEd2bHVBdGExRTFSc0UxZUh5N2c4WEVZeWdDWkl4NUZmanVOeGRSeGdIZlJrSW5GNUhOYlpPVXBXdVBqKzlZZWxES3Yzd09yOTVneFlkN0c4Uk1UdUk2Vzg1dzd2K3RvTFpIekJ4dTAzUnVwaG1oVDhjQi85QXNUci90cVlEOVoxU3BCY2g2c2hDVHZBa2dSYytNeTN2aUkyQ1p5cno3Z3VNRDNBMjJFTDhuZkJDZ0RXS2RCYVBFMWdFYndwV0pBYmdxL21iaCtBdVNoa1RDdVpVdDZDeTd0emFVS1AyZTJ2R1ZJM1hPVXlRdlV2ZkRpbnpSSlNzSkFXaDhVQ1U0UlZxVlpzM2YyRVl2UDU2dlBua3ljSHdycU1TZ24wMXlWTmNqME82NHJCQXJYYlRHZ0JxOWZZWFpxR3hkT2R1MnZtMERadU56YTRrSkdNM1dBQjd2ck8wVUgzTm02WkNVY1RkditWNEJBZFRvb0pGZitCUFQrbnphcXZwaXAvdHpkakJ1dVc1ZE9vaHZXMU5aQys0L09uR0YzaDRMQStzWk03K25UTVFnT1c4RjArWnJOT0FOWUgxbW0wK2MrSTFoS3lBbVF6WVcxc3NIaVM3RFl6NDZSMEVGYzZUa1RxRzJuZncwTXNSNUZKajRUcUs4T0dqL0hoTjVDczUzUER2QzJsOG1neThEY1FqZkxRNjA3cVZCRldjTXlwcFBHd2ZkWk9qdGxrTXRiMkVsaEhZd28zZFpaam0rT2p4eVFyZ29YdWIvUDlLV2RGajJiQzhwaDN1eUZJZHhnd2NXS09wRHVZT2hJcGh1b21vUU9vdmhHZWxQNHhDZU5XYTY3Y3NCZ0hwUnlXY3cxSE1pYlZ2TjF3ZFFVQ1pHVTFRTWs2SmtINWdvU08yUno3SWNMNjRsQXYyeUZtekFtc3dLVDFQS3hmeVhnS2xzTmhrVHlSSS96QU5ISW1ySTFic0VvTTBnYmpoUUlHQTgyNm83dmdBRGR1L3YyUU1lb29WUW1kaC9RUDNHeUJnWjhuZ3M4dGE5STRzbHIzWUxUMDYxdGdoYlV4TFA2NTliVklzcTRnZm5ZTVk0eEM5aVhBWFlTMDhXZGM0bWFVdDdjUFZnNUpzb0kwTE1QWXNoNkZkVG9GQzJndHNhQ0IvVHdEbHVWdVlMUjlCeWFjeTlRZGFpWmFMQ1pocnE0N0FSSGtRTGdDL290SkZZT1lzbHZFOCt2RC9aeUpOTFp3MzlHOFBScXBPMVI3aHp2RWF5WllNYXdPM25va1lMQzJkL1lPT29jTTF2Yk9Ec0k2ejhEYU1wSTIrZmtrNjR4YzR2dk41TWNwYjJpNFRJNnU3K1F4UkptTWwrTzRkMUZzRGdLR0JpQ0FNNXF3VmNqa0lSSWxadjN2R1RaUTBvZmJlUXo4S3lwb1VVY0lNMUlnTy9vZDg0UjNybTZ2WldBZEhCeHN4N0IyZGpvb1RaKy9PQWh1Q3BiSlZvazlEdXVQQ1p2Rm9xdkYxN1FtNVJISk1nS1VMTUlGVnAwMDhCdmdHck41VHdyUmdkZmRXSmRCWFRZd2ZiNlA3ZFBEdzNWaThJSFVBOU5HbXJDNG5jc2JjbHFXdzlMTEc5eWYxbVArQmpjcHIyWmg0V0xXUkxJWXJKTkRhblZZR1UvQlN0b3JuelR3c3BxRVdLQ0NTWFE2eTJiRnNCQ1hmb2ZXRm5tQk9Pa2NGNG9YYXFkRTRSYXo4YkVHeHR4aXNlS3MvclVoelFjTGIzU0N1NEdCTUc4QUxNZXpJYTRESithVUdTdUU5WVhCMnBtV3JKTkRpOVJ3L0RuakRaa2FjbURQd1lwakJsREVoTnhNQTQ4QktUZnBxSUIzSEJEYU1kVEdHNHg3dmhHdnNXTTY0QjBUUENoVzMxQ1VVdHpBWERGc2M4TUNLMThXTzVwc3VSNXU0bXNZcG02NVdrMWxxS3JWc29TRVBoOTI5dmIyM2pKWURrN29kTVpNc2pvcjNOaG5iRlpxYVkvNWxJR1hVckNXS0tjK2ZRS1dneUhDK0pyajR1YjloclFSWXZjQWhlbitoc1VQMTU1REwrWGg2UTFhTVc2NUlsWkE2dDlNeE9ZejhHd3NGUEpDaGU0RmdkM3RycUtpRjF5ckMyVlJ3emdKSlFzc3VRYURZQjBIdEprMEd2aVRMeXhSL0RJSnk3QWlvMlU4S1ZrSkxBb2FsbUk5bkFrTFRoOURLbThqTmwwT1UwYVNwZ0IvZXVEU1JlSUcyU3hZbUg5SGVOS3hLc2pZdjdsYy9XdXVkQ2Z0Rm9zQzJ5dk1FZXRjL1J4WjBuR25QSk1rNnd1Yk9ZQ1E2L3d6RDBReHp2ck1Fa1VXb0U1S2xvbkM5VFFzSjRMRlVwNzNzWVdmNFEzUlYxOC9vRDExRTFyU21Kc3VsQzVaSW5qM3ZLaUhzb2ZxdWNIRWlxbmpQWThrd0ZwRnJPN25uTEJJYWFNcUlpdUZvVnJEdTIreGxhb21pK0EvNDZDNlhpclZ5ZnlRdGxuUldwV25iTllwZzBXS3g0T0cwN2dDTWRQQTg1elZOZTRTUTAvR2l0c3FqQnpJZGpGcEkrMEVMYmhqY1hxS0Zab3JadHVSMWMxM3c0TGtweU5aVG9YTGxVU2t5SVpSQkgrZUdqR3FrMHpTT0NGWkpvTmxQQUhyS29ZVlFRSm9yY1hIWVBGWnU5dHIxN21MTEQwb20rVWxwaDErWlFSazZyazJidHdGek13eEZieVBUZnQ5SEduOU1DeWR3YXJqNmVJZUJHWUsxc25qK1RRQm5GUkRpeWFybjRIVkl6Vjh2eFJHeG1xUnVjUlphaGhOY1VMRUxhSEdjVnlPZ2NyM3dFMFZCSXNzN2twd3lSSW0yemYzajdENlFWaTZwUXNFUzVDWTR6Y3NEb3NFaWd0Vmx0bjVTYVNiazVMRkp2YWZnSFVSdzFvS04rTmlLY3VrWjVSbzlIZ0srSVlseitNNDV0SXhSN3ZuaGgyamVMUms5OGt6RUFwaG9CKzV3WCtqd2VMNlNMQyt6OEJ6eVRJZ3hhbXFGU3lRUmlHbHdXd1c1NVVsZFo1KzlueEtEUitIZFpxQmRaclVIT0lhRGNFNmZnUVdUVzlSMmg4SEVVNnNpbGdsMWpHcStKckdSZklXWnpnUkt4YkMzOHhWb3BtQXBlbUdYcTY0TXE3V2laTVZCaXV5VnVleE5KMmNNNEpNc000bllmRnRaNStFQldvbzlYQ0ZZYnIyeDJsbFlWMWtZTEhaaXB1eDYwWWhLdEs2MitCbUhUVFAwNWx3ZmIyUEFRWUk4RFlLUmRQcHpvOUpGc0F5WWtvUnEwaXlPQ291UitjUnNNVGVjMWluYVRXa1hYcWZnaVdEWkwyZllFVzEwc1hIWWNXOUw2NkxVWHBzdUppZC84YUZ5M0pkd25jZkFieUR4QVNiMGU0cGFQZ1dHYXdmaExXS3NMSUxNSTAwckt5Wk91RUVrNmRUc0FBUmc0V1MrUXdzeko0dnp6SUQ0b2pYejhPNjJZQWdJbzduWGNOeWdkWURvd1hDNWJnV0s5YkZ1TWgwWVpUUGc0Wi9zUURpUnd4OGJzWFNzUW9WcjhJMGFPc0dEbXZhK1VYUlF5eGQwZlI4WXJOb094R0NkVG94eVpxQ3RabWQyNGx6NnRtd3NPb1MwVUxiSGxsNU5GeHByNGh1MGJKWSt2ajFuajkxRFZlUklsdFdPYjJOK2thK0gxYXhMbHRVc3NzS1Znd3JIV2RGTnAxRkZGd25qd01uWGFLaFBqZUQyU3hjcEhyK0NLd1pjNnhFSy9LR3FLSFVHS0xmVGJVTzNTWldIbFVQUXRMcmgwaVFJT1l5c0J4QjlTZDZDdkFnVVNBWTFlQlJ5RzVJc3A0MzhFWEZlNWVNam1tbDE2MmlWREJZaDhkempVT0FoUjFvUjU4STFoYTFCY0lIakQvaGswZkhud0xuOE9pSU5mMnh2ejRkNnUzdXgvZEhNOFo2Vi9XQ1Q5VFJScTg4T2pyVXZlc05QaEcvc2NHbUxxNnZYY25iaUFhYWRUY3BUTnphbUhvSDlISVk5TlFJOS9mU1dZUzJrVFNHekRQSlduVXlpM2t6cktJQTNxQmI1c3cxSFA3S3dFQkk1aGFUclBqOWtIL0hVN1FCRzQ3aHBwdlhnbmpndy9RWEIvanBCdDdoS0Jyc1prYzAyQlpsRHZ4QnVXRS93VXR4MDJFYy9HZVhiV2FHTlFERG1Sakc4NUlGc0F6OGo3MDN2UVA5eE5BekE2S1kxR1BkWVQreFQ5QnBTbWRxQzN0OEFRb3FQMUFqOVYxNkpNR3BZOWZ4U1RBdjhOdjRxL2hiVGQ2WW1Ub2NoejRDdjloaDAwbkpJVHYwRHRxS0NaOWlsVHA4ZzhtNlllbEZlRUV0K2RuVlNrVlJOZ3d2MDlmMWswZTJjY3hqQTNlcWl3NlhGQjBPbHYvS2k5OXBzM2RlcHo1bVJodmE5ZVJYZk1lUjBadnBnV1ZKbldmM3hWaFFWaTFxVGZrSjQvYUcycUdpOWtQdW9tQzh3M094YllhSzlJRVZNOWovQm45c2NWMWh3SURVTzI1cU51Q2orS2ZkM0dhN0d5ZkhWL296OWR5elIzNXR6WE9UNmFJS3NNYVRQdWFlL291N24rK3o0K3RFaDlTa2Y2TFQ0VmVid1NHOTBra3pHUnpyOGNIMWs2c1F3ZU95UTlSdVp5OXd1RThhUktKQUxEclUxQ254dmwxMlJxa1hYYnZ3dmUrZW53a3IySkxoUExMRVlzNFJkYlhlM0d6d1hrVFdzY1dNaHBsaWs1Z1NLVDJpOWlVOU1qaFpjdHoySURlR2JZTkp6Q1BIOGZobG5ISG9oTy9tRHRJV2JZNXUrcUlDUnN2ZGVBTENVOTkwenluZDhJWk5seGxueG9qaklUUjR6elFTRkZReXpZN2FSOU1Ebit0b2JFbW5RMitSRVdDMEpJRUxIUGsrM2hINkVCT2o5Ukwzc3c3Ny9ybFRvSGw0dzVMbXVpUFdNcVE0em5Tdzk2UVF4WmtzcCtTNTJONm1KNkdHd2JyZFpOa0pQQSt3alBCbUhMaitVRzJ3eG1SLzRFK09nVitOT3J1cHNWdkI5bnJiaHMrV1pDWjJMQzNIM2VBTXJNWUhMbXRBaXNUcy9uc3d4WmNiZStEQkZZcHpiWHVFOXdnRVJieVo4NHZ1MDVqdW1CVG9YTkZ3MHhXSm1ScmMwNVhXcnphNnRENXptQjY0SC95c0VjYXR2RkUvTDNWbHFyUXVRUVJzRHJzR3VobC9uKzVFekpMK3lmbXZPV01WZ0JJNmMyNDRpUTdSY05MTFVKNytEaTVOWTJhMG1aSWdKZHhwR2pkUlZnUmN3TXJiMllsTUdEV0J0eVp2MWpCalpQYmNqL0dGUHNoZHQ5RnJLelc4RmFMclNIaERIMllJdVN2Zy9aUHpBb3RaNGNTeVBPZHQ3MTZWbGpYSk1vTHJwelNSZXhJbVR0UUpGYmw4d29RN1RhTWNOYnBNaHNKWWR2WmJ6N0NaZnlROTByU3ZQRzdmN09MOGs4NlYwM0JZVTBoTWJOWlpUTUxDdVE4d1dNcmNTOUpMQXB5N1ByN09mT3pVWStxc3d5WkUzVEc0Q3NDZ0d4UDIrRzFpaHNPUTM2ZHBsc1E4SlVwekUyUHlSdC9GTmdiSGZiMVkyTTZtd0hVdVk0ODNOYWRZNFJZV3VKUjRmbGJzeHBoWWM1MG14VjByQ1ZSaW5kQlhPYTZHbUZEZk9LVW5Ubm8vdXk0ak14amt5U1V1VHhQbWl4WHduUzFreGpibVI0Tkp4TkJqSmlMMktLNTc1Z2UvanhYckRhRUs5YlRzRWlnNEVKMkhrN29rNDYyLzZONU1UOWpxbVFyRUpDK0x5Z2Z4VU5WKzZqZnNwZnlOYzZrbkV6T1VNaS9BZVdHbWxWd3Bad0NMRnJFKzNBRXJYWnJYWGlXMHdNZ2JxZmJTKzFqenNLRU9yUUlHVEpLT25IcE02Y0tuVHdBQjhaZjAyVElER0YwLys2NVFyV0dJNzlsQ1AvcUYzOGFGTzcwQnZiR1A5NEpHZkxFVmZFeDhDZkVBNzJzZ2VxU1dHR0dZZXRTOE5XWEU3bW1EQVRCWDJydzNnSjJRTGNPbG1lOEVGTjQxaml3VTZWMmEwK1F4Ujh0NEdKOWh2NEVyNEZRa3NEL3MyaXpwQ2NCZGp2cHBXbUhQQTBHUVRQajhXcDgvMThmRjZwS05yeHZVUEp2Zm93QkNrSDZpdERPWjRUUE0vb05hMmtCTVlodndZU2pNZ2ZFazU1NnhDakMrMHBaL1lPZGdSb3ROSjVIcUFTaktWRkR4QWs5VTJnMjhSWHEwVVVQMk1CTmQ2T0pKK2oweG9IcUpyZnA0KzBJUGlLTkJRWE0zR3FSMEVPL0U1bmlpN2NEbEVKam1EVlVYcjQ2ckR2ZGJmVkduelFaWnNqUHloK29JZDJ0a2RqS2NLV1lzM2dCaW9KV2lGcEFWQTZlTnpZRFgxN3dlejFCUng5RDMzTFI2Z2hhNjMvSDFBelBtWkNyeGJvUjB6ejAvTWlPekZJQTdjd3E0NFNSOUlkQXgrc0YrSFBneDdOdTZhOWsxc0Nqd21VRnZHSitnUDVMcDVpZjltb08zZHFmVEh3bzZGcGVrOXJBVjlqM0owYU0rTDMwMERJVlZ2T21mVGNGY3Q5Vm5ON2liTWRqOVJzQWNDalU3d09BQ3JiN3VSQ1lNV2Jtb2dzNFBiMWRhVUJ5OEgzMHdScWRudVhDNjdHNkU3T1p4RTVkd1B6SERYWUZGMXlUMmJtOFlxcTd1T3FLZ2VJYXIyMzA0WjF1eW5CNFlIeFdla1pTRWN0ODJMYWZkUkFtekRBOWxxUlVPUk1uUU5BUHZOdFlhOUNEcFFiRXpRSS8xVWRNZlNTNUY3cnJreU8xbUd5OWppeXRuK0Fpd2ZnT0FRUXlMRWdhOFhKclh2OFoyVUZCQjRZZHZYbFNxbG5IS0h1dW1ob08zSnlSUVpJV203RU9JdHJUWEE0RnJEZHNTSzMweXl5WU1mVVZ5cFZwMUNBcEZOK0Zwd2ZsYnVncGdtL0FidlJicjRWQU4yQjJONEJXT3kwUU91Qm1TVXRNTnUwOFF3aVpJbW1sajdLNE8vWnFFaFdWSGxuWFRVWnVDWkVvanVJeGhGMitHTzFQQzJLcGsybzRXNGxlcWNLUEpkM1JTd1IrK20zUWhMeWlhdzYrYlBCWUZKUEdZbElmOXRnaXlGSGlqYmhpcURxYk42Qjd3RmgvZEljaUxFYUNZK0NQZHhWdlYrUWlyTjBRVkExaGlBcXNuV2N5U3R3QjhyMHZlb0MxWmdTcm9odHRsRzJrTUZYaHpqY1Z5VlJGTW5FSUtiK09kT3gxMms1K1dzZ3FlbzRXdmYrU1FVU1Y3aXUxSzVLNVlFZHY1d1h0aWxaYUZkNHJveURvbFdvNVhhM2Y3NVBJeTNxNUZFU0N4Nm9wWUhJY0xKZGxkY0haaVRhaGg1MjZmZkR6SUJzZ0x3RUlZSUJRaENJU2x0NGY3QTRJRjlLSlRBRENTNk5OMkdaRWk0ZTFJdFdvUCtQZUdFU3pRUUxydlVjZ3VRNU9NWkg5QXNPQTNMZFJtU1J5RWVBL0FFVmZNckw4bWxXeUJpNjU1RHQrazFuQ1V1ZTZQTzRrcVh4RTFWNWJ4M0hWWFZIcDlmNElVUlRFaGhuMDFzVXRlQ3ZTVm5CaWFvQUU0cDZicUVpeThHMllEKzV5N1ExUXBuQ05XaDhNUndCTGdIRUV5WEVtSUhRV3d3M3ZxUkdFVXdrTFNVZzAvRGV3NSsrYVJERi9DdHR6bzRwZW80WURkV25MWU13Z1dpTGJGQkhZSVBrb2UwY1hvRHlZOUVsNXQwTWpleUhabHVrMEpHQnFsOEwyNGloWHdNZGovTG5taW9BNmEwNEtNV2svQm5xU2paUm5DSlRYSWllbTRzeHk0cmRhd0FlZmhvbDFxaFEyNDRCckNRa2x3UVVKSWswYmRybHB6SXJWanpsQmhzUHE0YUx5aG90Q0c4TkZnOWxBNFJTYUJlS2NQRGhpcFJIZHRnNnN5N0xrV3lTbGNDNHRnVlczRENrQ0U0WnRGakhWbTJERzRNTWdya0ZBVmRmMjM3OTJQdXBUWGtEUXEzMkRDN2FGMUhBekN0b2czKzZJc1FoWWdOS2pwdE9rVE9qRVg5SXNjbTZianpTSFJSRGZJYkxOREJrbHFoeGdOV0s2bXVmQTJzWnRjQ29JMUFnbHhOYy96QWdITlhJMCtCb0lOdUFva2FpREZya1RmQVMvVThjWmh2azlpdUk4T3hFVFd1SHdVWVJGTXZCaHdPU21LRnVoYnd1elZCNVVQKzZwaVd4QTZ6RmZ4eTR3OGFKVXJRQ1l5bkl3M0IyQVdhMzEwT3hpY09salJVekhHMXEyZ0FjZmsxMVpkVjJDMkJwNHpCSEwrb0N3bXlodVhDZ1hVRGIwL0JhV3UwazJ1UmdTckxXTStKZUc3K3pZb1duZC9BSmZEN2JHckFFR2FSQThwQkROZHJBVkJZRHFnOEJWZ2hXcUFRZ0t3OEthd0Rob3hFREdzc2tta2tRTGVjWGhLdm54ZmdOaGhqaHVZVGc0SzN0WHBtQk1FSFFUSzYxTU1ZT0hkNnZzWUJZWmRqY01hOXZBNXJpMDZobzM3ek93WUZJQXo0MXhyTlJHV0s0cWlCMGpzUkxSQ1pyUENubTE3R01JM0kyRUowWnJwSEQxY0JZY2QzSkR1K3NwcWZTTEVidzBOWWNHemp1aWlnY2VMaUtZQTdRUTFlSUxZQWVGVngrTnBRMHJDOWpFazBjdmZ2eXQ4cVd6eEU0MTFyKzh6V0tpZVZUUWhjS0diM093VHJPajRJMWVOSjhJY0hSMnoweU5ZNVAwR3d6YklwZGdmREZTd1lkeFlwMkNCaDBEcmgzRHdVd3dSTWtIRm9hQURFMHVQQnlMc2dsZzBuMFltQ283RE1FVWZRbDZ2cmNraVhsUERzS3N0bEUvZDZOZ1cvSkxBTWQxc29TZEtWSElBcVpUMjdydFpnV2pCdDhLMzhJRWUxa2JCcHdzTnNDalNobk1aN0RNbnpHbGtJeG04VURaZXdCQTNJSlBhRVN3VHpnSXNzV0g3SVlxWWE0alY2SGdoZEVDSndNdURJZ2hXSEFNcHZOK1k2MXI4RE1sek1LV0dlQTFRMmpVYkxCeVFoRWlDWUttZUpEYmcvejZRaGc4SkdSOUpVWFRUOXVrY0RMSi8rMkViSFZpZkswM0RNN09iNmM0NzhCaGRsWXZXOEdPZ0czUU8vSXVxSWU3RmpZZEhDVVJySDJub3dnUXNNTDZteGl3VkpDV3lNbVJHSm9IVlovQXRMM2FIUXpMSWRHUGdFY0VLS1lrMitaMThtRWFCUmVJV3NEVkF1OWh1OWpFUjdVR1UwZ2Q5RjJsSC9hcEgyOHpybHRjWU1zSHlHZ2lyanpFaDZBRkV1UERBMDAzSmkySmRGTGdmc084NHdaTStlNUoya2h3T3EwWHFaUGVwd3FZMHVIMktZa3RXWjhLQWdhVjNnQWdpbzlxQXd3S0ZHVVpCMkJDc2pPWEdSZ3ZWMnlDRnJZb21SZ3d0YkpIR21VWE1jcGwwRHRWWWkxcFZEY3cyZmtYSVlpaUlGRXl4Q3Q1U0JWamlBSkludW5VdXVFSzBDQ01nMWlkNWhNeFVEVnNEQ0kwcCs0b2lFcEMrSDJEMTZsV0ZHUTh1b1hoOWRkSjN5SHdKRmh6eGx0ZHU0eFNCTEpES21Ud01DZ2Y5THYyTGRvMm5kejNITWozeTRDeGlEeHNlYVFKZVpST1NtUWdXaGhid1JmM1FieHRrNFBGblNhamkzQ0ZrU25UbmJoYmw5Nk9BMVhBYllWeWY5bTNURlBFMlhYNFhZUG1DVEFFTjZqSTRtS2JDbzQrUThvY3dSQzhqUlplWW1STDFoKzVrVWRWaVdTYzlRaWZmbzBDS1RDWWNwb2s1Tzg0eVJ3YWJhVk1mdDl2SFZKaFhFSWF0cUVyVm82Z0pBOHBXcXh2Qm9oZkYxZzVod3NXMkJjV0ZGTVFSbWl5eWJZS0hhU0p3RzRXUllBM0NWaVRuL1NTUEFWaUdDR25rQ0w4UlJBeGlHd2p2NEZwaFJBc1dqUExMZlF4Y2RJeHp6VmhFV1E1dmFqK3d0V3Vwa0ZkQWxweTQwRVJzVUxSQ0RvczJqV1JUSjY0OUdvUThYTWR0TlFOZDk4Z3hEakFBVllhUmlKdDJ3eCswV2JyVDZrZmFoeTl5RW11SEVTZllLRW1YeEpvQnNEQmdzQ2xuQXBtMjJGdlFZNDRTMzJ4WGgzRmgzZ2ZsMVRwNEpDaFptQ0xWZktvWVlpWUs0bThZWlBWNmtObU0wQ3lESVl1K0doMng2U3I1NzBwM1NxVmZsOThwSFp4SGtwUTR3OFhkUjBHbndoZ1dKaXdXM3JlaTErZ1BNQ3BGUWFBWTNXWXlDRzlpQ1crTGl3RGxqdkFQblNpWUZOMmhBd2RSd3hBcERxYTdOWnppZE1WdU8zRGJmbzBIREpRUlNBWUc3cjRTWFFPV1pYbDhDamN5cnZCdU1LWjlnS1hBd3g0WExMRHNQc0lpd3d1WFZoY3hKb2tORmdzY0RGUHFsTjh0NTB2ekFjdmxLb0tvR1N5dlRHUVVKVXJIQUFrakdmVDUvRGdiVVRXWExEeWRWWjhpTURUcklZVUpBeGJPQ3c3ZVhjYUV3Nkh0WFFkNGF3eVV2MUFWMnUxZUt1TUpmU3pXOTZyRGZxUFI5M0VmUUc3Ui9CRkVxdXNoYmkrdnJ5ck5mUjdBNHgxWXFEZEFpRHdSWHVTd0Q4ZUxoOXB2VWZFTW5ocEMwR2s0TEUvMHFBa0R5em10NUFTcGNVV1NEYzFXS3Jsblk5TmNJVjhwc3dxaVFUMGRiaU5sVE1BYVFpalkxOHdrV0FraW93WVJDNU5wTUdvdStVQlNFc2UxSXQ1K1QzU3hMSW1UTmloMTFBalNpdXFybVZTTlRmNVI1YUdWL2lYdXk0V3VveWNJSTRwcVFDeFg0d1ltdVFaMmZrajNyMGFSN2RPNTZ5UDRnaDRHNzFwTlVNRm1nV2J2VTNoR3JNUWtoVWZ4dzFrQnlxWWxwMXpKLy9vVXI0SlFVN1JWMllMRW5RcFkyZENKbkFWWUxRNkxSQTFkU29zVnRpQUhNVUR5K241L2hKWGpBZk9oZ1FrV2xYL0VzQTlTb2phaTZjQmtkbkY2cGpYOVRQcVhVZEVtcWJRMDJxeUFEWUVGeEhHNDR6WE8wT0VGUXVIQnF4dnVrMzdoMVhkeFAzVUdhMFRMUmlpM2oyQUJTYjNXbytvV29sL1Z5dUlqSmVaY3JxN1lta20xUHNteFI3MitUNWxTVXBaajJaV3IraHFHd1NFTFZyQXFTWUtBZFFoTWpTR1lCdTBNbUhjYmRrVnhGTS8rN2NkN2RqK0NhSTdCM3BONmE0anRJV3h1cEViV1NJQU1IRTB0YVJxS1Rvc0psa01MY0FFVzZTL29nVUdIbVJ3Q2hMZDRwemFxYmptWTRPdXlDZnFvVHVyalFqNWZFV25qRmdOSlVha1BrbU8wZ2w1cVRxK0tvbFdEdndrV0pXM29qNm13TmVxSEF3VVBRQWVuR1FoK3BEY1ROZDBNbmg5b0R5RlUwN05kWVJqTmxJU3FBb0xXdy9ramNOK1VlYVBiZHNRZUJEUUJ6cUxWS0JmdlVXRGlKeDlCQ1NjR1lTR2NVRSt4OFI1b0tJMk9XTW5uVTNjT0xnaWtmVGpKNVhpTTFINkxUN1M0S2ZnWVdZSm9pUUNMWXZFMjdtRUFWeFJiQ3RDNCtZS040dTZKdmNScVBpb2drMDFFMlRhc2ROdFdHS1k3bEI3cmtJajNIb1FyaEJPVmNQU09nNWtNSkZZWUF6VkR2OXRRREs0clBsNW91NSswVS9CY3grY3E0SGQ3STE1dGxsWTFoVytobnN0WElFcmcyb2U3SVB1cDZqRVlTMTFKU1NwNVBMRmptQ3c3eG9vdXdLZEZKakp6TW1xN3JVNU94MDlqaWtTQjdhZEkrN2JTemJVYkRUVTdHdGdUeUxZclJXZVFORWM4MVZVU1RZRmp4UnU5b28wVGtSQzN0aUNzTlRrUXpMK2pJalUvdHhFR3kzRlRGTWtYNmlPS3A2eDN3TjduWHEzWmZJSldCNHZlejA1UTR1UkMybDlnV0xpRms1c01GcFVqcWQ4QnUwSlVibm1uU3ZVcFhlR0lBQSsxN3FYNkhtbFBZT3JRell6QWpUWXJGWEdmVWdXRERKeTBCM1JaQXppYjJSQjdJcm9qMjVNRk5sbmJjMWdaQkoxVE9zU0tjNTFoOXYxOWxDL2VxaUN1dlNxcTJMVUdpVTI3T3pXVnkrWTVHMm5SR2xINHhXSGhyQXU2VGVwMm1IWFlzWktSQ0Eyb1MwOVFjTHRKVGFNWjJMZ2pHZnRvdDNCa09ycnBHVFBxMTZYaFVDc2hzYU5HdVQ0V2xDTnNVMDZWSFRTayt1eUtZeG1FbEE4amVDUEo1WmpqWms1MGdyZmZiYnVvT2xLblhuejFxcmlNMnc4WXJ1Q0hrd0lSVmFGU3oyRDZHMHRXS05pMWRnT3JaelBuZmx0c0xnc1pzV2s5a0IyY1RkUjV2L0hVblVubkdad2RFVGF3MVFaN1JDQitqM3JDWm5uWk9OS0E1RitYT3dpTFRGWTEvUnBCY3FXYVAzVWFZVlhCSUU1VytPM0lDb3FNMWYxYWZ6aEpLNXFYU29aUFZWemRacy8xVVc5bmlSUmhHckRPUlRhTkg2MmV5Wnc1M1hVNnhqYXhhb0RkU2pKK0Ixc2ZhczRBaHlWbG9CYjFyVTUzMUNTUCtqMUJ4T2tSWnJMU0owWnlJVXlWMFNINHdUbHJPUW00Q2hVREo0N3N5Wkk3QmVsbU9uZ2daWWZnTHE1cVRFbzlOV0Q1UG9VOVNJbEowU3daTWswcnUrckpZQ3V3VTBUWjJoU1QxcURUZGhDbVpWcXp4eGJ2OXRYc0dtODlqRHpvNUVYMElaSEZKTnZKcWd3VlFhYm1IUHdlYnBnaXVlbUo2dEp2NERFczNXdFBxT0lNcXpjVUlHQ2RiaUpnQmRPUVI0ZTJSZ1pwSmlYcUo0dkVLa3RyaThtWmxjQ0sxcCtiekpNOHE2VFkrcXV6SG5Kc2RSaUVrMjFqYkNxMk5jQkdoMFlhRnJkbVdSWDBCVndpS1lzVGQvMFEwSENaRGlTYldiSzFxV0x4b0QrcGVUU0p6M3NTYlkrdnBYamt4TkRnV1Z2V0pBMjJmaWUxT0ovclpmVExMZnBqVHFscStwTXp5RkExMDQyYlV3WUdvcTcwWEZpb1RJWVNtSjZOTUU1WUxVOVc1VXZMQ2tUNFczcXRtMzREWlRqUk5IRDh1Vk9LUjV4cU9PY2FDeE5iOGpBRmF1Wkptc2txSi9QcEY5T25QczdLeUVxZmlSdUlvQzFEWWo2N3d2dXp6NFBsT3UwTXJHRkR4R3hFbjlYOWtGTTBORnhlTHhOcHNjblIyV0VCZVpsQlh5Vk91aFQxVVcveEhVWG94MGlJbUx4d3FlR2FsVnFrT2Juc2MwSUwrV1BDWWRBa29abThBTVhZaUlERk8wOFlLV1RVUk0yYU9jUFpUZVpEWHJlZE5sZWFNTFAwa0Z2dW9PSENHQ0o1UzlYbUZhQkpWQzNXRmNJN1c4MW9BWlBqa0dnWlJrcWJrSm1MWVFQTk84RHA0QS94c2t2UHM5ejRVZlRRd1EyQ1VrOWJPSm52bWlSYUV0Mk56RUhURnI4Z0VVMmlaVzdSc21UczZXVGtxRHZmSGpHbG5BNmE5NXNUR2tUbUNsako5bVBMZm5QVnNvd3JWa2I5UkJ3eFZKQkdrMDRDWTlzR2RvWHdmbFpXMURBOHZJL0oyT1RHSnJNS1hlbDJJWkRwUWVEdW1kZ1Bvbmk5THQxNFFlMnFXZzMveFdFSGVGY1lYTlFrUWhhcWREL1NMUVZVRGUrMTNPMjJxVk5NRjlVK0JDVUNGb3dwRjFJYmVMZGN0bnVTeVZmQzR0R01jYk5kajhKdkpva21hOGI3Mko5dXBHUTlQY2xwRHZzMXVCcUdYSzQrV3RJcUZSVnErclFUUDhFU2hINVNWcUdXZ0c0YkxUazFsMXU0TTZCbTQ0U1U2UjFkWGw0U0xjdklXR3BMYnpjaGQvSzZUV3cxMG9WbXMrMVZteXpSYTFZOTdEekNPMU0weGFEUjVCa1JwQW85ZkJwZTRIdW1BODgzS2J2U2ExVTgxWERZZGlVRlh3eHZhMkxMakpWRUl4aWNtZU5QY0RCSDNoYXVHdXhvbWl6VHpsODYzczJzSmpSUXdoS05uUFQ2dzRZdFlUTzhVSHlxdWx4VVVmaFNob3ZtS2FLNTFuM2VMWWNWVDJZMTRBUEZzaUQ4VnZnTmRNUjBEck8wckJTc1ZxdW5lOTNXSUlSdkVDQmU5bnJVVDRRcG9nZTVlcDlMbGhxQ1YrLzFjWExZYVBPbndYaEFIanpZcHprUHR4RzIxRm9iZmhKbHhlZnZFL1UwTExvbk9tTjE2S0QvV0NuOGRxZ29ZbW9ObEl2eFQ5d2VsTXhRODBIbVNuYWVhNFl2VmpWTWhJVDRmVFR4U08wWUxGR3FZWjlSQWtHczVJdUZCVkRoT21RQlZoRFRvaWdnOFUxWVVPMDUxQ3JhRitIVHc3WU9XWE43T0d4RHFvelR6bzB4TnNVYkxxVGpvMVZIb0s2YzNqRHMwZllEMkZiVDZ2YWJiZDB5YXdOc2lnZnNvUUN3d28rMDBVQ2s4UXdXQ2xQTUtscEFYOFNTblI3YlVDQkc4L1pEM2cyZHpWVGduTUdFVlo5dGhpL1dzZEtlMGtPY2lxb053UEZTeDZxZXZTOTV2TzFCQ1pmNndCcy9KYktGS1hBaVdXSFlvNTZ6d1ZDUUNaYUZMYVZZUkxJSWxrTVM0UUVzeFRGRWJMRENDTHRISzZqeGFnM2EzYWJxWVdVMmJMaW9Nc1AyNnFpRlRkQkdLdUxIUDF1NEcwa3NWM0NJMGRSV1RwSFN4MDRtVEtFbUlHcEZTSFVmVXM5WC92bHVtbEpkVG1mZnJPSEg2L3FxWUx1UjdpVWpOZGxOdEl3TUxUTjI0M29icEVSM3U4TitGN3cweUMxT3lqanRGQ3hhYmtHd1JxczRWUVpwRlFKMjhYa1Q4cEMrMkJqMmJRT2VwTTZVbWlEVTVGRVlOcWkwazRKbE1kdWV5RldTcWVRU3lZckRDcDE0VVVkUTFOZUJrM0dlYVRpVjUyZkQ4RFlEcVE0Z1p2dU1HcTZPbUJHVXArK3lPeTFiWmtvTkFaWUVzS285M3hjVmFtQkl3NElNdDlkVDJ5Nm1JRzF4MUcxaE5CelpMQUZiS3JyakhnaUFnN0N3OG9rMUIrd1VwUmUwM2RUK2VMUVJNZDZVN1RESXNucEZiUkZUd1N6VzBVVnJJdGVwU2U3cUhMQUtZTE9DZERaSUpTeGR0MllPVTZ1bjNpb0FyYTAwclFRcVNnbDRRL0J5WUhwaXlRcEREbXUvaGM2d3E3bFlQTU9WQVcySW5RQVd1c2FtNm9BSXFUSzZCY1Bwb2JoRlloM3VoK2dNcXg2UFl1bHY1bWFPR0t2MDlNeXlhODRPLzAwOTA0dUdmVTlZRVg2K0E3Q2l1NWthR0ZiYVoxd1AvaTFiVnVhV3hFb2lXNkFFR1ZoY3NueXgxK3lDL1o2RU5jQ0lxWWVTdFQvb0QxcURIb05GeFdZQlBFRkxXQld4UlVEaXNMQThqZ3BBTDFDOTFKV0pXSkVPWmxvZUJjdDRJaFZQUmQ3VUxXYzU5ZWRZb1Y3clNxYTQybldqYkdRcnJpSWsvREo5Y2lSYkNhMnQrSFdSZ1FkWW8rYWdOd1dyMWFCSW5Cb1AybmF0aTFOVkZocDQ2alVDeHpCb2o0USs1Qk15R1RJanRsbXFONDV0RnJYK1oxaGxwLzNleFllenRZVi9MRmFGWlRuVFJLNkRYVGJLY3haKzJaWjRsMnZxamNtTjgxaVZQSlhrNnRrRm41eFdwQWdwYnpqa3NHencrdjE5Z3FXbllRVmJ1S3MrZVVPWktMZ0doaHM2WENCZEhOQUtndjE5WDBGbEJBUHZ0SnROVlZid1pabE5tTXo0UWsyektwWWx3K1Q1Tmx0REY5K01ERWFteEltejc0WWtQdGNEK0E2U0xtMmlFczFtWDlqOEMyWWpEWWdoRW5lWTFXeG01Zm5sL1RTZUlWazREYlFmU1ZaczRGVzJOUWpCMG5YeWh2RHJWcHZxc2dyRWp2NWdnUFB3RUZ2QnIwd0poTE10S1VNSWNTVytEbzlVa053Z1kyVk1zQUlqSVRHdXBxYTBWVFp0MU8zSEk1dlB3WGZvMmpNOWdHaDE5TWxLZER4dmgwa0pMaENxYVpFNDY2dVRkN05OMDByTVBJUGxka09BaGUzQ0xNNktZWVd0THUwVm9tRlFxa2g2RkRvQUVQd3d5Q2Q2dFZvTnNvQ3VhL2YzQnpYSDd1NVRVTnJxaXBCVmkxcmF0TU8zRXF2SlpUajV6bW9VK3ZEMVd1QlVlQS9CVkZrRmV5R3NkMC83dzJKWm5tcWpaVGtoSklVdFhOeFkwMWhPaUNWSjdEK2Z5c29MRmV5V01lSnJ2TVZoZ1VxNURUd0lyeEVPU0dRU213V3hLbDVjWDNGVmJGUEN5OVcxZFNaWldDTENURWVYYmNodVJMRHcrLzFlRjV0RUlYUmc3eHUwWlpTWklHWmxXdnIwa3FWQ0hmSTBPbXhXdDRFSXZ0ZjN3OWtyWUxFN1ovWHBSWnE1WmNjd3ZNbjVvSDFxS0tBQTNtVVJQQ1RQYnFlc0NOWENqRDZtZ3VCS1JDdGx1Q2lSMXNrYjZxaDFReVpaVFM1WkxIc09td0pFaGo3QXdwNkFta1R2c1dqNTF3QnlhR3psaGZEZlUzMUlubkhTQlJKcG1xd05td2pMNUY4STBveGxuT21XamxLdVZLZ0xpdGl4Vm1YcW5ZZWNXc09VWnhCT1YyMndkVUd5bjRaVldaMmVEMkwxbUpHTmRhc3RJbVdBUUFuNXdxT3BFMVgwSXpNUEZ4b01hcTNkVm5SRGFiYzdwcW0xMjIzcTV4M0J2MWl3RTl0OGlCYThRdFJOVDhCWHdIdktDQXR2NEVaMkhINVpjK0NYUFloRFJaeHNid3ZzZmVDN0luUDFpY0tyenNxanVsUElLNVZ5UjVaUndRMmR0UXVwL2FuSlBPenJlSG9QdTRMSTFyV2x6QlVyTTNnU3JkSUJEZFJkc1NKRXQvVisvSE13UE9VbUJJU0xablIxYTB1aWp6RWxicEx4eVMyMmZwUFB0cHE0UVVyMENqMStHZTY5YTRDRjVMOEVSMFpSc2k3TC9IMlJDb0xhMHd6RGsvRmtybGpNbzBycXJCb0hLb25MM2libW1ERW5scDhNSHVyWXk4ZXI4SHcxSGhncFErSkhCbElMcWxlYzQzN3dCV1VWTXRwRU0rS3k3NHp3OXNsZHhqSXZqQjl2VGM0YXNTOGljMlVZcTNPc3ZzbmxpbFc4WllJbGswcnFFdmE2TmZxdFdDRXBBMzFxQVVGTzBTTXR4RWFTUGliUEppc1p5NnNBNmwxaGVkNTFMY3NWdVBhbUdkbmNURGcvT3h0SUQzTStmaEhFU0t6SVhJRnBuM2VOUkc0NS93NXMyS3JNeTZtUVZKT0EwV3c2dFpSWG5qaERiR1FTV0FNK2J5RXhhTVd2NUNtVi9QZDE4UllVTGFPS1kzUG1pUkthMU4vc29SZmxMMmFTakQvS2o3c1Nwb0tHckNtL2ZzZGhsZ3I1ZkVYeEpHb3V3dVh5OW9nMWZWQzNxaTQrWHRMNlRUTk04RWUwdExPRHpUWXVkdHRnTjZyNmZEdnExR0ZVUkxKemtTcnlJR0xHaUdkU0l5VnoxaCtjOU8rMm9pcmlET0J3T2NpeW93cENDaUNKY3hRTEpnVXNwMWJLMlBVb0dXUVJOV3k4UWozYzBqdVBDaW5tS3JwWXhUWXVpOEkzTUZNZFJmbHQrVWNYV1pjTkRKTVNKY0Zac3EzSFdabGNmc3pEWDliSFp2YTNqOEV5eDdFWHdmbGJSL3lCSmM3ODVCWEZ0bGJKZ09tV1YrdDFzVGIxZUZ5YXg0alVWaUJHSUZLU0pDcjFmT0g3VjkvRlkwRjFzZi9maWZVa21LV0xFN09GRUZJOC9QTExnNTc4TnJsVnoyUm5TU0pXbE9CSTduZHVTSjRadVVKKzVTM3JWc09xb0RmcW1FK3MxTVJiQ2hnbWIzVURZMWN2ZnIveVRkQ3FpbEpHdUdib1ltcHFkWXZOUEI2dS93S2k1VGpSQm0vSlN5YzBNTEZXWTV3bmxPejZ3cDg3M2xLdVdGZHNsemRDd3RjOXVnWTRKK0RHTTJDbnNLa1psK3ovb0VCbnZqMWZZWFkrT2kxd1dHbGMyWWxwc096dkhoN1dnUlhRT25wNGVBZWhxQlZYOHFOMmgvaTkvQkl3c1lKWVdjai9qQ011TFF0S21WcHNjVWlQeU9xdlpTeWlTN0pUcnZ4VytJRmRmbWFQZkY2VXQxSm5ocnFZd1dVbTl4NEFXQVFxR2tjWXQ2Y0I4WWUwclR4bzRGRVNzMFBRWHY4ZUwvamtLQlorcTVRZEdmT1YxZG16OTZWcWdOb25WcXJGaFo5d2haSlJFSEF5UG9wUVVSZlR1Tkp0TktELzQ2T0UxY000Nm1pWXRuT0pxSkpZNmJwUytKTXFtSVd4VUt3TEluYVgyYk9ObG9MYUI4cjNVMG5STnd0bHluS2NOSzQwTENPQ3RZWDNlWXBaQlpqalRGZUNjZHFVZnhMSktWaXIxVCt4Mjg2alIxMHFMS00rdnBuRm95aFU4ai8xNnFTK2wwZW9xSXV4UEpoYnNSb2FLVzlvanJrbUhvM04yWUVvMlBWSXBha2FBMzViK2RHQTRabXhVTWhYWnNadGtCeS95QmZTeU5YTERub1lNem5QR0pmQnVnR2pRRDZHTlRQT1FQbE1md1Npc2l2RmwySEZzTHdjbGNkR3FTaDAwQ0hIZmpHeVhVWkt2U3dXa2JLeDdwbVBvSXAxR1crQ0lHdkNDNkw2MjBhaDRzcHAyOHh3eGR1YlIwSDZOWEM2ZmdEeE9weWNxOXhLM29xaGxZTXozWWJ5OHh6M2Y5UW9DZ3JscXhsYzQ0a3cxWHkzL2pEV3BmSEQranQ5U3FveXFLaUY1OTMvSnFwWEdDQlhSQjBiZmVNZzZmSUl6anNUZUJuWDF3NjV6dXZyVERlY0V4eW1VVkhKc1BOU2h2MC9aQlRyNHFxZXdVWGFtQkt2S0ZaSTNSa1k5UytMQ2w2ejJsSCtUTjc2WHpHS0JkV215aXRwVlVvYk0rS1ZzVlJHa0FZN3BpQlVjcFQ4LzZ3R3BrWnV1V0xyM05SbktVeXZNZGphTW9MWVVqRVp4RjQ3WFN0Lzk0NWcvNjBqdHl5VVpYblNFaEdLREM4a2xmNDlWZGd0YzdVakN2OVhVT0VvRnQ2VWFRcHlhMUs4d05wdmNmVkxrZUtTaDV2Tnk5ci9EUVZNajF4T0tMdVNSTFkreUVJQmMyOGFHVklvZERSeEFnb28vTStiOVZtanRMQ3NkR2hwdHBrV0wrS1YrcEVMRlhhY3lyS296RDIvOUQ4MzhvV0t5T1pkbWN0TFJDbGx5RGdwUXphVXlndGwrdjhsbzdoUVVWeVQ4U0x4aWdVcUlvVzlySVlrR1pvaUxQeGZzMVZUbzFSY2ZpY2FxOUtFT2g0eDYwV3JXbkR4dTFLZFl5TDgvOElvRllTM1lMMG9MdUR5aGFUWWRzZjZxcXdvK1o5ZTIvc3ZIcmxpdmw1MmRaWm5nM3RFa2FLNWNOMmhXYWEvKy9qKzAwWXVweXBpaDdhaWdoZ0xhd3F5b1lsS1pmblB6c2Y5cjQ1Qy9wMmlyZEtpRWR6VTVGMytmN1JZOVpNRzZHTkZST2VISzZqK3YwZzlPM0tRYWY5bkt0Ly9BNW1wZWxIbjZBWG1BQUFBQUVsRlRrU3VRbUNDIiBhbHQ9IlZlY3RvciBHYXRld2F5IEludGVyZmFjZSDigJQgUXVlcnkuRmFybSI+PC9hPgogICAgPGgxIGlkPSJ3b3JrZXItbmFtZSI+4oCUPC9oMT4KICAgIDxwIGNsYXNzPSJkb2MiIGlkPSJ3b3JrZXItZG9jIj48L3A+CiAgPC9oZWFkZXI+CgogIDxkaXYgY2xhc3M9ImNhdGJhciIgaWQ9ImNhdGJhciI+PC9kaXY+CiAgPGRpdiBjbGFzcz0icGFuZWwgY2F0LWNhcmQiIGlkPSJjYXQtY2FyZCI+PC9kaXY+CgogIDxkaXYgY2xhc3M9InN0YXJ0Ij4KICAgIDxkaXY+CiAgICAgIDxwIGNsYXNzPSJsYWJlbCI+Q29ubmVjdDwvcD4KICAgICAgPGRpdiBjbGFzcz0icGFuZWwiIGlkPSJjb25uZWN0IiBzdHlsZT0iZmxleDoxIj48L2Rpdj4KICAgIDwvZGl2PgogICAgPGRpdj4KICAgICAgPHAgY2xhc3M9ImxhYmVsIj5FeHBsb3JlIC8gQW5hbHl6ZSBWaXN1YWxseTwvcD4KICAgICAgPGRpdiBjbGFzcz0iY3VwLXRpZXIiPgogICAgICAgIDxhIGNsYXNzPSJjdXAtcmFpbCIgaWQ9ImN1cG9sYS1jdGEiIGhyZWY9IiMiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj4KICAgICAgICAgIDxpbWcgY2xhc3M9ImNsb2dvIiBzcmM9ImRhdGE6aW1hZ2UvcG5nO2Jhc2U2NCxpVkJPUncwS0dnb0FBQUFOU1VoRVVnQUFBUUFBQUFFQUNBWUFBQUJjY3FobUFBQUF3WHBVV0hSU1lYY2djSEp2Wm1sc1pTQjBlWEJsSUdWNGFXWUFBSGphYlZEYkVjTWdEUHYzRkIwQlB5Qm1ITktrZDkyZzQxZkVwQmZhNmc1WllCQzJhWDg5SDNUckVEYXl2SGlwcFNUQXFsVnBFSjRDN1dCT2R2QUpIanlkMDBjS29pSnFKTHlNVitjNVR6YUpHMVMrR1BsOUpOWTVVUzJpK0plUlJOQmVVZGZiTUtyRFNDVVNQQXhhdEpWSzllWGF3cnFuR1I2TE9wblBaZi9zRjB4dnkvaEhSWFpsVFdCVmp3SzByMExhSURJWUcxemtRNk1YY0ZZZVpoakl2em1kb0RmWE9Wa05iYUVLY1FBQUFZVnBRME5RU1VORElIQnliMlpwYkdVQUFIaWNmWkcvUzhOQUhNVmZVNHNpRlVFN1NGSElVQjNFTGlyaVdLdFFoQXFsVm1qVndlVFNYOUNrSVVseGNSUmNDdzcrV0t3NnVEanI2dUFxQ0lJL1FQd0R4RW5SUlVyOFhsSm9FZVBCY1IvZTNYdmN2UU9FUm9XcFpsY01VRFhMU0NmaVlqYTNLbmEvSW9Bd0JqQ09FWW1aK2x3cWxZVG4rTHFIajY5M1VaN2xmZTdQMGFma1RRYjRST0lZMHcyTGVJTjRadFBTT2U4VGgxaEpVb2pQaVNjTXVpRHhJOWRsbDk4NEZ4MFdlR2JJeUtUbmlVUEVZckdENVE1bUpVTWxuaWFPS0twRytVTFdaWVh6Rm1lMVVtT3RlL0lYQnZQYXlqTFhhUTRqZ1VVc0lRVVJNbW9vb3dJTFVWbzFVa3lrYVQvdTRRODcvaFM1WkhLVndjaXhnQ3BVU0k0Zi9BOStkMnNXcGliZHBHQWNDTHpZOXNjbzBMMExOT3UyL1gxczI4MFR3UDhNWEdsdGY3VUJ6SDZTWG05cmtTT2dmeHU0dUc1cjhoNXd1UU1NUGVtU0lUbVNuNlpRS0FEdlovUk5PV0R3RnVoZGMzdHI3ZVAwQWNoUVY4a2I0T0FRR0N0Uzlyckh1M3M2ZS92M1RLdS9IODZ4Y3N0VjB2M01BQUFOZW1sVVdIUllUVXc2WTI5dExtRmtiMkpsTG5odGNBQUFBQUFBUEQ5NGNHRmphMlYwSUdKbFoybHVQU0x2dTc4aUlHbGtQU0pYTlUwd1RYQkRaV2hwU0hweVpWTjZUbFJqZW10ak9XUWlQejRLUEhnNmVHMXdiV1YwWVNCNGJXeHVjenA0UFNKaFpHOWlaVHB1Y3pwdFpYUmhMeUlnZURwNGJYQjBhejBpV0UxUUlFTnZjbVVnTkM0MExqQXRSWGhwZGpJaVBnb2dQSEprWmpwU1JFWWdlRzFzYm5NNmNtUm1QU0pvZEhSd09pOHZkM2QzTG5jekxtOXlaeTh4T1RrNUx6QXlMekl5TFhKa1ppMXplVzUwWVhndGJuTWpJajRLSUNBOGNtUm1Pa1JsYzJOeWFYQjBhVzl1SUhKa1pqcGhZbTkxZEQwaUlnb2dJQ0FnZUcxc2JuTTZlRzF3VFUwOUltaDBkSEE2THk5dWN5NWhaRzlpWlM1amIyMHZlR0Z3THpFdU1DOXRiUzhpQ2lBZ0lDQjRiV3h1Y3pwemRFVjJkRDBpYUhSMGNEb3ZMMjV6TG1Ga2IySmxMbU52YlM5NFlYQXZNUzR3TDNOVWVYQmxMMUpsYzI5MWNtTmxSWFpsYm5Raklnb2dJQ0FnZUcxc2JuTTZaR005SW1oMGRIQTZMeTl3ZFhKc0xtOXlaeTlrWXk5bGJHVnRaVzUwY3k4eExqRXZJZ29nSUNBZ2VHMXNibk02UjBsTlVEMGlhSFIwY0RvdkwzZDNkeTVuYVcxd0xtOXlaeTk0YlhBdklnb2dJQ0FnZUcxc2JuTTZkR2xtWmowaWFIUjBjRG92TDI1ekxtRmtiMkpsTG1OdmJTOTBhV1ptTHpFdU1DOGlDaUFnSUNCNGJXeHVjenA0YlhBOUltaDBkSEE2THk5dWN5NWhaRzlpWlM1amIyMHZlR0Z3THpFdU1DOGlDaUFnSUhodGNFMU5Pa1J2WTNWdFpXNTBTVVE5SW1kcGJYQTZaRzlqYVdRNloybHRjRG8yWVdZek16QTJPUzB4T1dJeExUUTFNelV0WWpsa1lpMHpNamRrTURBd01UUTROV1FpQ2lBZ0lIaHRjRTFOT2tsdWMzUmhibU5sU1VROUluaHRjQzVwYVdRNk5tSTBOVGcyTURndFpXTmxNaTAwTlRZd0xXSTVPVFF0WkRnMU9HWmlaV0UzWkRVeUlnb2dJQ0I0YlhCTlRUcFBjbWxuYVc1aGJFUnZZM1Z0Wlc1MFNVUTlJbmh0Y0M1a2FXUTZZV1V6TWpJeVlXVXROelJsWkMwME1XUmtMV0ptT0dFdE1qWmpPRGcxTUdVM05tSTBJZ29nSUNCa1l6cEdiM0p0WVhROUltbHRZV2RsTDNCdVp5SUtJQ0FnUjBsTlVEcEJVRWs5SWpJdU1DSUtJQ0FnUjBsTlVEcFFiR0YwWm05eWJUMGlUV0ZqSUU5VElnb2dJQ0JIU1UxUU9sUnBiV1ZUZEdGdGNEMGlNVGMzT1RneU1qQTFNalV3TkRjek5DSUtJQ0FnUjBsTlVEcFdaWEp6YVc5dVBTSXlMakV3TGpNNElnb2dJQ0IwYVdabU9rOXlhV1Z1ZEdGMGFXOXVQU0l4SWdvZ0lDQjRiWEE2UTNKbFlYUnZjbFJ2YjJ3OUlrZEpUVkFnTWk0eE1DSUtJQ0FnZUcxd09rMWxkR0ZrWVhSaFJHRjBaVDBpTWpBeU5qb3dOVG95TmxReE5Ub3dNRG8xTVMwd05Eb3dNQ0lLSUNBZ2VHMXdPazF2WkdsbWVVUmhkR1U5SWpJd01qWTZNRFU2TWpaVU1UVTZNREE2TlRFdE1EUTZNREFpUGdvZ0lDQThlRzF3VFUwNlNHbHpkRzl5ZVQ0S0lDQWdJRHh5WkdZNlUyVnhQZ29nSUNBZ0lEeHlaR1k2YkdrS0lDQWdJQ0FnYzNSRmRuUTZZV04wYVc5dVBTSnpZWFpsWkNJS0lDQWdJQ0FnYzNSRmRuUTZZMmhoYm1kbFpEMGlMeUlLSUNBZ0lDQWdjM1JGZG5RNmFXNXpkR0Z1WTJWSlJEMGllRzF3TG1scFpEb3dOakZpTkdNNU1DMHpNRE01TFRRMU56Z3RZVEJoWXkwME1XUTRaVGd3WVRVNFkyWWlDaUFnSUNBZ0lITjBSWFowT25OdlpuUjNZWEpsUVdkbGJuUTlJa2RwYlhBZ01pNHhNQ0FvVFdGaklFOVRLU0lLSUNBZ0lDQWdjM1JGZG5RNmQyaGxiajBpTWpBeU5pMHdOUzB5TmxReE5Ub3dNRG8xTWkwd05Eb3dNQ0l2UGdvZ0lDQWdQQzl5WkdZNlUyVnhQZ29nSUNBOEwzaHRjRTFOT2tocGMzUnZjbmsrQ2lBZ1BDOXlaR1k2UkdWelkzSnBjSFJwYjI0K0NpQThMM0prWmpwU1JFWStDand2ZURwNGJYQnRaWFJoUGdvZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0NpQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQUtJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQW9nSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnQ2lBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBS0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lBb2dJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdDaUFnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FLSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUFvZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0NpQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQUtJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQW9nSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnQ2lBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBS0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lBb2dJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdDaUFnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FLSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUFvZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0NpQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQUtJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdJQ0FnSUNBZ0lDQWdDancvZUhCaFkydGxkQ0JsYm1ROUluY2lQejdjQVpBdEFBQUFCbUpMUjBRQUFBQUFBQUQ1UTd0L0FBQUFDWEJJV1hNQUFBc1RBQUFMRXdFQW1wd1lBQUFBQjNSSlRVVUg2Z1VhRXdBMHM1dERVd0FBSUFCSlJFRlVlTnEwdldtd1pkZDFIdmF0dGMrOTk3MSszYS83OVR3M3VnSDBBS0FoQUNRSVVBVEJRYVJBaWhMalFJTWwyYklwSzZYSVNVcE9KVkZWS25FcVNsWEtxa3E1TXBUS2tWMnlMRHV5SlVzUkpWRXlKWkhpUEFrek1SQUQwUmk2Z1FiUTgvakdlODlaS3ovMnRQWTU5NzUrVUpKbWdkMzkrcjE3N3psbjc3WFgrcjV2Zll2KzE2Zk9LY1AvVWxVQUFBR0F4aitZWDVPK0ZuOVIvanNCVUNKQXRmc3o1bnVKOGovNjkvZi9rRjRxdmg0UkZPcC9CZ1FsSGYvK1kzNHBBZjdicWZ2Qncxc3FGQlMvVVFrVVBvRFNLdGZldW1nRndLM1A3YjhqZlBiMlQxTDVkZnQ5Q2dWTnVxQnh6MFJiOTU4dzVoMHgvak9vVG56R3BQNHpLZVdYWi9qSDZtL3FHajVqZk5seHowckgvSDB0NjY1MURXRUJBVXI1YVhSZWN5MXZSaGgvNS9MUGFyZ3Y2VDIwOWFNMGJ1MU5lTDlpditTdnhhMlR2MzN0OTNyOGZhUFdodkovck5oOGoxK1FHTHRwdXdzcWJCVE5GMnBmV00zVnBmdlN2aXFhOUNBcGJNandDbUdENW5mVzdqTmFaWE9XRDhEZUVEVTNuTXcyVEd1cHVIK1Q3ckxtWlpFV3VvWWZJTkx3WXVVOXBMVEpUYUFwQWlpTldVQjJZNW9yMHZ4ODdWNVlVL3hRUlhHajI0ODRMbkpTaUJLWTRzL1FLbTlRYnFMNDNJbnlzMDJMazhZOG12Wm5wMG1QT1VZbHpRR1VZdUNrNG43a0h6WUhEaWpGc1B6Y3RCdFk0d2FJNjFuUjNmd1QxZ2xwQ0JiYzNvaXJYRnU0SmlKQ2pETlFBb1hJcTJ2YytQWWdMNEs4K2NYVVBxN0N0Wkk1ZndnRVNEdHErWGRRUXJxNEluT2c3dlVLSnAxSzFEbXg0d0lqUXNvSXlENURJcFJmeENwUmU5eVh0ZnZqWktJamRWOWFKejNrWXN2YUc2SFFlQ0t4MmVUeHZwbG8wRG1KV3g4SitTWHpmVzFkQUhXZTRWcFBpdkpGaXVzd3daY2hJQ2lJQ1hBRUZCbU1wdjhWcDR6SkxEVzhPSkVKdURSNTg2eTJ0eWlkK0JwZWk5SnhRNjFnV0M2Y2NVdEJVMEJHTzNzb0YwZzM2Ym5oR3JScElYVitoc1pkc0RtUXlBUmdYY1BtdDIrallZMW8zazdoM3NkL1YxUkt4ZEpOaXpuZU1HcmRHcnRlWWhxVWtsWTFLM3RNR2tmUUl1VWY5NmhUNEExWmlDcUtoeHMzcnFRejltLzZ5eCt0S1ppYms4Qm10Mk9EdFk0NTU5UXUrUFR5NlFRMUIxVW5PS2JGdklhUDNONWYycjJrOU5CakdURTJHZEF4MFkzS09CTU9IckFxcG1RSUxGNUh2YlFJNnZkUnJaL0RzaHRBaWNMWlFLQ1VUVkEzZUptc1VVbkhIQ2JhdWNtcmxVQnFUblExTjVhS0FFL0Y4eWxMcnZDdlJEN2hWVjFUSk5JSjU4cWtoQ3M5MjNIcnZwWElwZ3l4ZUg3NXMra2FsZ2FaMXlWYnBZekpEQWxBUlRlcUxjSkdaQ0tJVFpualF6S1pOSVducm1QdUh6R05UV1VuNXdLdEU5SHNvSEgzYyswVlVseHMybG1rYW5hQXJxRXkxQnNrSFdwTElnb25rMnFSb21rWitGUEFYZTNqYTdnM1NtMnNJRzhraXQ5bnFoMWVyUXcyYnl2cVQzbDJERzVHb0N0bjhmaGYvUmsyNlNKdU9YUVRyaTBzNFpYejEzSG5oeitCNlcyNzBCQ2hGa1ZORG5BOXFBaTBVWERyY0VtSGhRVVVNSzdlYmErRXlTV0dYWCsySUl1SGlCS2x3MTg3ZFliUFMzMkpzc3A5cDFVV21MYXpKZ3NjMldjNnBxeEF0eUxWbU5IRVlDN21zRjFsQXhuNExGY3I3ZkxjTG9ad1U2cTE3a2hOZ0JIQTNWd3NYU1JOQUdreXdLVTNLcW9uZ2xpVGdDMWE4OUhaQm5OV0FTalhWa3gwZjRiS3o1c2ZnSlluZmd3NHF1V0RHM3U4VUFsaXhjeUZBQ2FHcXVTblh0UzBaYVFjdTRhcHV3aUpmWjdJVW9NdnY0MUgvdUMzOFE5KzVtRnMyNzRWRjgrZng3cVpEZmpFMUJUKzNlLy9JYXIxR3pFVWhmU25zZVdtdzlpNC8xYU1xbW0vU0xURUFMU1Y4eEoxMDlhMW8xdmxKWWdwWGVNSnNaWkRRU2NkNlIwMGxEckE4ZGhrUVduQ1BmWTF2QmJQUkx1UGdBME9OKzZqckpKcTJDb25aenNtaDZjdXZrZi94MVBuOUYzZGR4cGZqMDZPbmxTQWdCcEQyamdFMjRJVzRnRTAxVExNcHJSV0o1VVRFeER5dGNhSFZhSzlqa20xYmdnZXQrOERsYVdFVG1KSjRwWHFHTWlSY3VubFNER1FJWnFWSlVqVHdEbUdxM29RMTBkTkRnMDVnRGdrUFdxeUhKMTRDZ2tEcElLWmVoNWYvVGUvZ1ljLy9rRTBJTXd2THVBTGYvNFhXRjZjeDJkKy9qUFlzbmtPMjdkdnhjejBPcWdvbm52K0pYemwyUk80OVVNL2lxWCtOSWdjWE1wblZ6dGQvZWxHNWpoWFV6NnY1VkVXZXpMa3Y1MkttYWpEc05nc015VkRhakJpRTV4WGYrOGNnSW02WlRPTkE0NTBmTGE5bHNPSGJwQ0JLbHBBNENUOGFtSUF1RkhPMnlwcTA5S3lzRFlSU3Jhb1JZaWwrbDRNQXpBR2dWNXJWWjlPbWpIc0Q5MG9razZtTEMxQTE4bVl4OVhTME5ieHJ3bm91L0hKTTRFZU04eUVRZ0FJV0JXYmFJUm52L0FuZU9Kclg4VE4rL2RneTViTldCbzFXRUlQMnc4ZHhmN2I3d1kyYnNXSXFsQUdtTkpxVXFBam9KSVZYUDdlWThEYko3RC8wTTBZcXFBUndXaGxCZlZ3QmJPekc4RHdCNzNVSXd5WGwxRlZQYno2K2h0WTJMUVB0Mzdva3hoeEQ2d0tDSlcwQlkxaGF0WWNvOGN2L1hmUEh1WUFrTUZDbWx6a3IvSjRkSldLUVEyZFdrU3pjVm1RNnBveno4NXlicEVTV21Dc2s3UHVhaUp2ampLbEhQdXVKbUw3ZEpOemdVNDBobm5STHBWSWtXdkdHQlI5YlFkNC9DV3FZMEhsVEQrVjd3c0ZsRnUwenBpVm82czlHQnFYaWxFK1Bjd1JIck1XTmJDbTNxajBNdmRZd20xMTJtQlFMK1BDYTkvSFM4ODlpV3VuWDhmTzJmVVlMaTFnejQ2ak9IanpRY3pOemVHZGQ4N2dxMy80TDdIbnZROWk1L0g3c0ZSTlFScE5ZWmhhejVNQVNQaThVN0tDNzM3OVMvamtoeitBeFpWbGdCMElBUGNxVERtSGVqVENoYk5uOGR6ekwyQ0ZwN0Q3NEdGczJiVUg2Mis3SDdQVEczeldFYzlHV3Eza0trOWdIcHZnZHlEeWladUZ4aEJSWlJDUG4wckNad1NJT1B5YnJpbWQwelZzVUoxVWFyVndEeDMzR1ZzSDJVUVFzYlVPN2NIS0JnOVpMWDBabXdFa1BRekc2eEJVYmNuWml0NlVvZWl4TW9zQXpLUWExVHd5Q3dpVElaWW14WEY3TXBkbFJranhjaUtDdElNZ0JVaXBxeDFPTnl3dnRGV2pUOGcyVXFTbkxnZS95bGtuQWNoanpmakJRR3VzdlBNYXZ2SUgveGYyejYzRDBXUEgwTzhOME5RalhMcHlCVytmT1FPUkJwczJ6dUsrOTc0SEIvYnV4VmUrK2pWY210cU92ZS83RUZhcUtaREV4RVFUcmtORUlGSXdNeUExRms0OGc5T1AvQlZ1TzM0bmh0S2dDZ0ZBb0NBUkRCZXU0NXZmZlI0UC90Um5zRzdiSHNEMVVJUFFLRU5ETUdhaTh2Uy9FVk5HdVg2UEVkU3VzN0dWcUs0ZEd5cXlDRklvTVVoelNaa0JQRFhBd2cweUFFdmZUZ2hNS0RMZ2NuMms5YWZhQ1R1NldpeGFiYkhxR0pZSjd5SUEzUERGSTdwS2s5T0xsSUpvVHEycFJVUDRHOXlTcnJVa2dIcWpsTCt6b2FnYkxIVk1EYTFyaHo0bTNoNmRsTytOQnpVekZWWFVBUzB0WUJRNitYc3NzWFJRUVI4MXJyMzhMTDcrKzcrRmozem9neGl1REhIaXhLdFlXVmtDZzhHVmcrdjF2V0NFUEMxMys1RmI4YkVQUDRnLy90eC93UHJiUDREK1RjZWdydGVwVk9KblpGS3NhMWJ3OWQvNVAvSGVvd2ZSRVB1Z0VPNnJRREU3TmNDNXQ5NUNkZk5kNEFPM2c2bytPT0FWYW00dHA0ek1GSDVHZFVscmdJUUxjVnE0UTJLeUY1MGtyaU5UZXJiRVZXV3M5dmNaRTUrbmp0VWx5U3JwZnJ1a0pKT1R4UGNyRDFmdG5QNXJxdjFYb1hMWG9wNVUxVFd3QUdPUitNQ2hxcjFPTFlxYVFuMDNBZTBsYmxWL1pEbkxWY0Fxc3pDb0RiUk0ySHdwVTJpblY2cyt4QWxnbnVwNDVQOEdiQ3Bhc3VlTytJZEtTUjhsWU15RGNuVDFITDc2ZTcrRjI0N2NqQ2VlZkFyMWFJU2wrVVVzTHkxQ0ZaaWFtc2IwekF5bTFrMmoxKzhCUkhqMmhlOURwY0ZESC84b2Z2UGZmdzczN3p1SXBhcm5OeUJKc1lGWUZRTVJMTHp4Q3FicUpWVDlQdXJoQ0xVSVdET0t2WGx1STA2ZE9JSGRtN1ppcFJxZ2FUM2JyTkdpN3VtdFJ1VzRDdVNSbUN6Vk1kRG9EWm5TTHVsVElPSGhlazNxb0dNWGdJNTlyRGRhSzRTUzlXbXJEMjNnSlh0NHJlRmdXdFBtWHdPckV1OHJqejFOYVhLeGt4NjByZk9UZ0lFNnI3VWFVaisydHRZU0lhRzBtaWdEYWRSZExCUGZSODFuU3lnd3RhNlhVcjFObE9WU1JOUjlYWlUxb1RSa01CRDdtYldsTFM3dVZZaW1FWk5RelpyN0thN3g3YzkvRmh1bUtqei93Z3RvUkxGMzMzNE1SeVBNYmQySzNYdjNZbTdMWmd5bXAwQUVqRVlqaklaRGpFWkRQUEhVczNqenJYZHd6OUVEdUhUeUZUZ1ZNQWtZQ2c0YnlrbU42V1lKMTE1N0RsLzVnMytOMjI4L2h1V1ZGVFNxRUJVMDBxQ3BHelJOQThjT1c3Yk1vYW1IWHR4bE40c0NITE5wN2FJK2JPN3B1SFdpWTdRUjdUL0g1NmtUczYzSkphTVZCNGxHZFdwTStYWHNRMDBaZ2xrYmRxMlFsZGVOS1czaWZhR2tSU2pWazZsc0xzcm9zUDV1UkR2cjZxYzlqYm5mWk5aMjFkMTRSaWxscVJCYURSNmhRczVhVUN1clJiUXhOODJteUFxajhUYTBIeE5sRHQwaXFHeEVHRGRLeDgzR1ZKT2F0Mm1oY1hlK0MxaE9EbTRsdXVzZnRzVFVzN1dDUzJyVFVKL2FZUG44V3pqeDZEZXhmbTR6UHYwelA0K1pRUTkvOHU5L0J6djM3c2JTNGpJV2xwWUJGVGdPS1R1elgwZmlPWU92ZmYyYitORlBQWVRQZnZtYitNQ1JPN0NFSHRoNTBMWW5ReXk4OHpxKzlCZC9qRTAweEk4OTlFUFl1V3NYK2xOVHVIejFPazY5K1FZYXlYTEpoY1VsSE5pM0h5OWVQSStaN1FjQTdnSGlyNFE3c042TmcvNWFRRFI5RjY4ejhkKzAxTUxZTXBNU0c2Q2R6TE9BajIwbTJTcmx4aThIMDl4V2dOUTVqMUFZSmMwTlNvQk8zcnJHazc1WWsrWnIxVVF5bUNiMGgzU2xWeDNOOS8rYlgwVnFiakdadFZCb1NwUDVUc051ak9OKzgvZmNPQTJqRGhDNFJnMWlGRklGK0ZmYlFBS1ZzQzZGUFZlcDRQUXJKN0R2dHJ2dzhDLzhFbEN2NEYvOTJxOWkyNVl0V0p4ZnhHaDVCY3VMaTFBUlVGV2gxNnZRNi9kUlZUMVArekZ3N3VKbG5EOS9FWE04d3ZEaVdmRFd2U0Jwc0c2MGlHZSsraGRZT1BVOGZ2eWpIOFZnM1hvc3JhemcydlhybUs0RkIvYnZ4OXpzUmp6MjFKTWc1d0FWWEx0Mkhmc1BIOExYdi9FMGJqbHlEK3FXQkRtbDZoUDZHMVpiblA5Ly90S0FiSk4yMTFubnpMQWJVVE9BVGUxTlQ5UTVGc3RTUEFPNk9YQVlmUXUxc0pqMjVyOFI0Zjl1cm4rTWRxWmE5V1J1eVJvVFlGRjhRQ3JTM2Y5UEhtVHFIaXM3RVRTZS9oR0VWTzFJeVNmVlJKcmtDVnJRZFZFd3F3WWdXdk1WRUVxOStaaDR4RzBzUXlleDJDaFVmRWdiS3FEVk0zUDRzZi8wdjBJenFQQ2xmLzBibUoyZXdzTDE2MWhZV01DNjlSc2dVb09yQ3ROVFV5bi9IdFVqTURGSUNhNnE4T2hqaitQZTk5eU5aLzc2YTdqM1IzNGNWODYvZzgvOS9tL2p2dHNQNCtESFA0YVRiN3lOSzlldkFjVG9WVDI0WG9VWFh6bUJlNDRmeC9WclZ6RzdjUTZBWW1sNUdZUEJBRmZPbndXSmdDb2tvWk85ZUNHZHFBd2R0L0hiQWVQZFBJdkUva3g0N1hGYytXb0JYbElQUjB1YzNhclZNeEdVc1NHNlVkWnV0Ymx0TVVsN0wrbTcyVFpVWkNoanM1VlZkUUFkWUlIV2xwNGxmbFhYQ0ordkxROW9QMEMyTFpuUUx1QlRnSTdVamljbEQxdVVFT1BhKy93TjVCc3RtQmlzeG53ZkdacFVnVzQvOXNSTHR6UWdNQUpoOXcrOEQ1ZEZNTGgwQVM4LzhRaDJiTjZNUzFldTRzR0hmeHAzMzNrY3YvRlAva2NNK2dPTXBQYU1BSGw1c0RpQUdnSlE0OHk1aTdodzRSS0daOTdFWDMvMjMrRE5WMTdHTFljTzROSzFlVng0K2prNDUwQWdjT1ZMRDFIRnFLNXg3dHhaaklaRHY2aEUwZFFOcEZGSVBUSWJTenU2LzdGdHpMWTF0WlU5ZFFHL3lZRzFMSm1wVEc4VERYMERUNFVKNjFHS21LeEZreFZRTmc2bHcwa3hVVEZZUUdyaDBHSlFZa3lzbjBGdW14OWZ4YTVLMEkxTDl3bXJaclFNcktYbi9XOVN0YTJCbUozMHRRbm9qcWlhVHJzU0NjeW9jN2V4WXl3MXUwcHJKYVdPY2lyKy9xN3F6YkYzWmkybFJWdHhTQ0JpMUFCRUJXZFB2b28rQTVldVhNSEhmLzZYY05QOUg4V2pUejBOQmpCY0dhSWUxV2pxR25WVFE2U0JOT0cvdW9HSzRxOGZleExPTVM2Y2VoWDc5dXlHS0tOdUZDcUFpQ1FCajhkeUdwQUNaODZjUTcvWEx6S1Y1ZVVWS1BkQTdIS2xyTG9LZHQ1OUZxUTBtYXBkN1Q3cERaNkIwbGhXaUc2NDhMQTZkcUFGRTk0dFdXMEoxS3FTTytBbWxVRkdOUjhWL3MvYVlVVFdMSXpWeWZkcWJBQjQ5MGs3RlVoaW1XYlFSQ0toN0wwZFU0d1ZYVzJTTzlzU2pHcEZQMXJVbkJMVHRsVUN5TGdVZERYMmdNaWd6YUF1c205UlZ1b3VyMWpDZE52RmRReTFZZ0JkSW8rVzIyb01oQjRUcnB3L2k2WnVjUHpEbjhDMlkvZGlTUWl2dmZRaUlJcVZwUVVzWEwwTUVZR0lvSzVyTktNYVRWT2pya2RvNmhxajRRZ1hMbDNEM05idEFCRWFGVFJOQTlXbWdEaDlPbDJCWFlWbm4zMFdVOVBUVVBHTHMxZFZPSHYyTEdhMzdZQ3dTNnlGa0FFOVNjZWl6eGpESW5XNStmSElMWm5PUHJTZVlmdS9HeW5nMXJLYjJxK3BMYWFzWUNlMEZkeldzbE81L2JtOUt0SENqclFHcUttemhtazhDekFoQU9oa2h4MmlNVzJVeUZIS0lQNGRGb0U4d1VKa3RtamExSzBnUURLRzhFR1NaeXJVbjA3aUY2Wm9BNEg0eGlJVktPSi82TFMyanF0a2JnZytHZVVXME9xeUdrdDNLbHBNWTBqdnRCUDU3Wk9rUUJJUmpjOU95UHl3LzRwQXBVSFRuOEx4ajN3U2k2amd3TkM2eG1pNGhHMjdkNk0vTmUxVFNWR0lxS2Z2bWdiU0NPcW14dlQ2R2N4dDNRcGk5a0lqRlg5L3cvc0lmQ2JRcUFKTmpRdG56cUFlamtEa0lJMS9yZG5aR2J6dzRvdllkL2cyTk1UWm5LUVZ6ZHByUXkyb0Z0Wk80WHlFU1MzamxMNWZXMGV3Wlo1VU0zV3FxcDdXMDc5NWVsdStibnVIYXpkbnBMVW52cW90QXdiemZtUDlSV2p0eklwUzl4b21zVm1jdWNaeGJLdmtqUnZKQ2hybkJqWEdhVVZiNmhnYWM4b1hkRmNNQ3BKYmpBTjVtZ3hJV01FTUVEa1Fzd2U0MkF0VWlMMndCU0Y5VXBKWXpTV2pDaXEzNGhxVUZtUG9HQnAzbHBOWjJHMHJHVXIvaTJrdmRmeS9US1lRVHh2and4ZE5JVlFFVTFQcjhJRlAvamhrWnM3ZkcyWnMzclVIQzRzcitMR2Yrd1ZzM0wwUDB0Um9wUEVsZ0lndkFiUkI1U3BzM2JZZDdMSzlGR2tNcW9xbThWbURxRURxR3BmUFg4Q1RqejZHL1FjT29CSC9tczFvaEUwYlp2SGRGMTdGMXYwMysyZWhDcWM1ckFuUnhJS242N0trWFk3YUZtRkVsaVRySkhmYU9hMHRsVzVWb1dYTE90M1F4bWRzWHBxRGxOanlNT3dOcFhDQ3Q0OWhwQ3l4MElGbzZldG1uMzNNWXVKYW9YZFJ1dEFZWDhKSjE4WGhFRTBjam9aR2x2THJYSnpLdWZOUGM3UlZtQWlzU2FhcEVnTTIrZjhFVURHL2EzZy84LzRJcDFMMC9tRUlYTno4VURnSW5BcUl4Rk16Q2tBbEFCb1NIb2dZa3lyL2dNU2NEdjQvOGVZVnhoZXZPRWtrUi81MjFpQ2FYMWNMK2thVG5pRFNmTEhHaXppR1FETmliQVV3RmlBenA1MkcvcjlhR2RNNzkyUFhuZS9GVUJtT0NDTXdicm43ZlpqWmRSTjZPdy9oeUwwUFlGZ1BvYzBJR3VwL2JRUXFpaDI3ZG1GaGNRSDFxQUVIaWUveThoSldWbGJRaUtKcEZESnFvSFdEZDA2ZnhpUGYrallPSHowSzErK2hybXZVVFlQcFFROXZuWDRMdDk3L0lFYURhWWhLeWxUaWRYT0xabFhLZFcwNlNVdnJHLy96b3NVekk5SVdybUJCMzFJaWJKK2J4U1JzajAvS3lOUUtjblFzVUtRdDNDS3Q2VkFiSnVEUnZLZS9Ua2xCSVQxYkFTUXN2cnovSmE4dmxPc1NBV3oxc3VFSkdUcHVVRzZZWFUvbTgrZi9mRUNzQ2lvejBtTDJBa25NaVVYRmg4bU5md1FSMDEwV1RuUlN5b2FFRkF3eXdUbHRRL0NOVU1zZE0wQmVldXBQQU9QZFJ3SnlBQ3VIcEo5QzZrOWcyM0JCQk1DbGJDQitKdUtpcWlvZ1ZkOGw1NHNpVG8xSmRpSEhhdzhSbWltZG5qVEdPc3dqMVZyVXZlT2F1aVFZckJSOU1OQ0FFZ2ZkZTlTUXV3cWJEaDMxWGdrYzZDbkgyTGovTUQ3d2s1L0JGVXpoMkE5K0JDZWVlUklYWDMwQkhBSk1UWXpaMlZrc0Q0ZVkzYmdKaTR0THVIenhFbHl2d3V5bWplajMrcGkvT28rbUhzSVI0ZFRKMTdDMHVJaERoMi9GcHEyYlVUY05XSDBadG1uTEhQNzhLOS9DcDMvNXY4Y1Nxc0pkSnE0UUNlQlN0cVJpYjlaSlhXUStaVG1rUlQrSVppc2NJSmlSQ3JJbUpFcUtFMlpSbUdCcVdnY1dXS1B3Yk1Wa0hSWjNzZDAzUmUrRytpWXAvM3cwMFkwY2NBWnVNV0VLTFdnbllrNFppRm9QUTlia3Vrd0dmQlhMVnhKNXRTYm5QUlhWb1dwcDR3NWxiMFIwVUhBczFUUkxCeFdhbTRFbXRrS2JCMng3OU9PTmpNbWFoTWpub3hmQTdPdkxFb0FJRFo5VU5va2svRE5hWjBGQ3I3bS9LUlY3eFpxbEFTUG9aRS8xUmswUVVYTUNGTzNGRERWQkxSbU1TT25Vd25aVFN5UVhzdWRjS2s5RVVybkQxTUx4TmFkL0hROFk5WDM5L2pWejkyTFJXMkUwRDRnb1I3Q3ZTZ3JwR00wSmFCVG9rV0xkY0FGUGYrWFA4TlFYL3hUTjhoSTJiTm1HZlRjZEJCSGgrdlhySUdMTXpXM3kvUUlLTEM3TzQ5elpjN2g0NWh5V0ZoZXdZWFlqZHUvZmk2MDdkb0NkUzRCa2p3bHZubm9EdDMvcXB6QjM1RzZNd0JuSjEwelBDUmtLTUxvV1NjNkdJbzdTTVljSk41ME5mbUkxSVZaOEZWV1V5ZVNHamRqRmZwNWtMbU82VGhOSXlSQkllRTh1TlFnY2ZzNlVHS0t0UXkzMWJLRG9IdlR2S1Q1Z2tYcXN4NHJRNG1kSis2VGwzS09semlUcWlEM21GY0llQVJCSjc2L21SS1lRV2VOblR2ZVAydjA3aEtvVXM4UTYzMGNzZjZwVDZwdVBkVTVHaVcwbTRCRHRJV0dRNnhqaGtsUTRDMkZiVmtVWi9XYjRpTzlZd0dBNFVqamlmS29RQUdVMENqU05vQWxkYWt4QUUwb0pDZmJRU3VFZFErdG4rTWNVOGVJRFpxWVU2d1N0RmsvTzlWOE9hdjVJaXZjcWJtb2hVNytsUmFFNVZCckNPSlpSSFVxVGpiVlVZUmhCaFVsa3doNklQR2luaWlFQTZhL0hEMzdpeDZCWEwrRFVpWmV3YTk4QlhMMTJEVTFUWThPR0RSajArMkFpekYrN2hndm56K1A4MmJOZ0JRYURLV3phTklmdHUzZGozY3dNcEpIUTFzdFlXWmpIcVZkZngyMGYreFIySExzYmkrcXNURFBMWFVuRHlXaENvV1RsbzlWZnhJMFJuNzFBdzhhVGtMSDVlbHZhRGxJaE04enJFZEFtWkdVVTZ2UFloVmVZem5DeVBDdk1Vb012UUFLNFVva1NjNWxJYkZDQnk0QTVaNjVHYWVpeEdVcXpLSEkyM1ZZQ1dybTdCUXkwQUo5enR6bVo1akFrMXlOVjlwbEpLRGZBTVNoSVd1TVVoQ1ZLNVh0WFZHaVZLU250YkNtQWtNNHFPSDg0QmxRTmlxMzVobEJxY3RBRVpLZ3hoQ1JHUXAwNWJnS1RDanBTT0NaVVlQU2NRNVZjcUJVY1NnaFJoUWhRczBNajZuM3BpTUdOb0E1QkxLWmlNVVVUQXB3UW1EbXJDSmxNV3E5cGswVThva3dyUFJCSktNRSthdE11RnNrUFI1TjJqQlZLbjJOcVVZSUZCYWpaM3cyRjZFb054S2dROHRjMlhTL2lHMy8rUjNqejVlOWp4KzY5dUg1dEhsT0RLVlFWWWJnOHhQbTN6K0xpeFhNWURvZVk3dmV4Ym1vS3ZmNEFNek16MkxWM0h5UTUvUUwxeWdxdVhMcUl0OTU0QTNkLzdEL0NzUTkvQ2d2cTRKSmF6aFNXclRReUdyR0FLYVBiYXB0clN1UXFsWU5zYk1UaUNXdFBUa1AxUkp6RmIrcndkeTRiZGlrdDJMSTExOWZCSEpLOE1XWXk0V2RTS3MxcU1zRHNicHlOVDlxMllJYktvZEk3ZzFoQVFyN1lJOU1OWUMzMzQra2RTMm95cnNwcVBCRE5kUktYS2w0aTg5NFVEeEZqQ21wWmUydU5aTVV2Q2l2MUxOK3dvOXVrTVQzNUZKdGZ2RHcwR29zeTVUcUZTRU9xcVhCRTZCT2p6dzQ5OXFtbkM0NUswV2xXVk5Fd01GSkZMVDdhMWswMmcxVFZkT0tyMWRhVG9mTE1aL1kzbC9QSllhY2FVZDV5NmI3RVU3MmpPZ3dMaTVIODAzTjVFMm81alg1L01ZRHFHSG14b0JDaVVxbEpqQnNwaGdNTzc3dXVXY2IzdnZ5bmVQWFJiMkxIbm4yb0cwWGxIQzVkT0llelo4OWdaV0VKZy80QWcwRWZVK3Mzb09yMTRLb0tSSVNOVzdiQVZSVlVCQ3ZMaTVpL2VoMlh6cC9EMVlVRlBQRHczOFdSRDM0TUM5VHpKem9Jb0NhdmdzVGN0RHRMdzJMbFlEYklrcWMwVVQ2ZDFacGFGaWkvR1B3bWErajladUgwSFBLNlZZREYrUDJiYklRenFCcnI2T0xmNDR3c3NvY1ZwWFEvYmtBN0IwTmJQZmc1WnBSMndTWmtaUWFldGJUaE4zZ1NBZDBaRFNsalJIR3pOT1JJRm5TbVlyQ0ZZWnUwQ1IvU3I1ZHFyYnpsV0tLY3NxMUpVZjBhSGlLZFYrcEJMUmZRdXBqT2svb05IenpLUVVUb3VYTHo5NWpnUW9MQjRTNUxlTTFlSXhpeGdvV3hRZ0V2Yi96R2JSUmdjWGx1QVNGc3VtenlvQmJmSUVtbmhMVFFWVzFwMUJMQ3F5MHJjN1F5QkFyekVySkpkeFQ2cHM1QVRndWQyd0xUc1NwRkpRTkttdXZvbzhFTFgvOEN2djFIdjRzZE8zZmlqUURtYWRPZ0NwbFViOTAwMkZXKy9tY09nSmhpTURYQSt0bjFPSGZtSFZ5K2ZBWGFORkN0SWRVQWYrdVhmeGtiRDkyT2xhb0gxREpXYm8weDk4V09lUE1IZURPRzB5WmprZDV0SU1vOGYvdzdKL3drV25rVjVpRElsRnFzbXpOaTM1VVRxclg5SWVud2FSckxqUUNpeGZVZFUzQTdUU2pQTzlEV2U1aHNxTlVtUkJac0RvZWpGWnloeTRHMDZKQkFzNlcxSXdtY0hLYzU4K0I0UmwvWGJBalNVbmw3emgwb0wxeXBBRHNhay9JdzU2amxpQUp2N0NNVms2LzVLeUpVamxBeFlVQ0tIclAvT3dESFpFbzBTaHA5Y2k3UVRvMi9wUzVzUkpGRVFTSGhNeHhHWElrWmRFU3BaazNjTURYR3Jxd3QzdkkyM0tvWjNDbWxGU2c4L3lrRUZvM3ZxTjFnVVZvemFLdmJvWlFXTUxKR0lLZHo3RGVFTnRpeWJnck1qSXZuejRKY2hYNnZRdFd2b0tJWTFVT01Hb1ZUSHo2NGN0NUJ1R213YWRNY0xwNDdqd3RuenNDRkxHVjVWT1BoZi9RckdPdy9naFZoVUczQVVXTzluWXNsYzBKVG9IZ05VSXlPdlJlMVFtc0dCc2UxbGx1SnBpMWRvNGNjR2YySnQwcm5QRm1LMEpFcysyL2tpYTdXVkJ4aTlnVE9HSVJRYTh6Ym1JUFNLaGlwblRKcmF5b0V2Y3ZOYUtkbmRXYWhhZG92a2pRRnVXUWpyREVBbE8yUm1mUlY0NjVUK3Y1SEVZczNuSEFVVC95dzRWbmh3cWJtOEcrT0dCVVJuRk5VeE9nVG8yS0NZNlNmNVJTUnhkTkNVVWdZM29uRHd1Q3dBRGx3OFkzNjl4TVYwL3BEL3VmRFlrazBVS3VFMFlnRkZCUUtDdkFIYWtkVGxXZTNkbXpURkhsQ0RpeVIxZGtVak94Um9TSDlGRElxd1VnYlFkQ25Ccnh3R2M4OStSaldyZk95WFdrYWFEMUN6UXhNYjhEN0gvcmJtSnJaZ09XRjY3aCs2Ukl1bm40TlowKzloc0hVT3Npb3h2ZGZmQkhyQnozd1ZCK2l3RzBmK0FnMkh6cUtSYTNBMnZqZ21jRGRqQnRwMnl5Q2N1dXBXSDVPN2VDVDByMlpXd0t4U1QwV2hZdXY0UksxYzlUeEdON1ZqTXhSTFNSQk9rWUsyOTc0YW1ZM3BCSWlVSklSakNYTmcwYTBjRHd1cWVmY3FlY3pSSnJramZGdXVtdkh6Qm1Mc2dzUGlJci8vSXlVMmF3OUF5aHNzWFAwa3NSdm9taXM1ckNSbUFBWGFsUy80WUhLTVNxRXVwN2dBMFRJQURocy9Jb0lGWHYzVmc5c2FNUmpNNExQWGxpVDgyakNWQUNEbUJnajhWbUk1UHdBa2dXQ0FVdXdyc0hTSFpKR0dnaFBOalJUMTdRajN1MjJKa0RSR0k3WkxNNndTSVR5NlJsUFRjOUlrS0VqZzFRM3V0YVFGcE52K2pyRXl1bFg4ZGwvK2V0d3k0dm91d3BMb3lXb0twYUhRK3o3Z2ZmaG9iL3pDM0NiZDBIQmNPd2ZmSDN1Skg3cmYvcHZRUUJlZnZVMS9PVFAveUkrKzl2L0FyMmV3NUxOcG51SkFBQWdBRWxFUVZRdzd2MmhIOEY4WTJYTENzNWtmUExtSzFKVnlodExnektPSUQ1emlvdGM3SW1weFp3TUxRVFl1dW9pTDJlYW12WndBTGJITXg5TWJKaUJ0RkM5SUlmYXlKYTJXc0cwbURYWTdsbklTa1pPYXlrZUpJWHhlVXR2VTJZbjc3N1JiS0tLWFF6N0Z0K1BiY0QxR1VPMVNwWFJPclBpN0RkemFrWEtKOFpMQTZyNEM1ZXd1UWs5WWpnR3FsRFRWd1JVeEI0TUJJSFlVMzFFbnBLckFOL1NHdVdLcHFraEE4S3hoUEI3MmpFaE52QTZLQndCd2o0OXJvVlJpMkpFNHAyREpJSWxGcm1aNVBKT3BnR0l4bWdyTlRLVHB1Yzg5RGhvemh3aTQ1L0VJc1JoTjNqeGgyTU9WSTRGSDhUWVNibkVDZ2o1RkhkYWg3ajAwbmZ4eC8vOGY4UHNvSThHUURNYW9YSU9seGVXY1AvZitsbjh3QTk5Q3FQQkJxeUl6NUs0WVJBMWNOVTBHZ0NMQy9QNDBiLzNpOWg3NTczZ1AveS9VWSt1NDY0SFA0YmU1cTFZRWdEU2hMeXFBTkFMVXd0S09nQXVrWDNVYUM2ZHcyQjZHanExRGlPNGxCbXdTU290TGtoR09ka3hXRFp1cGtvVDVLOXFmV2UxSmZ2TmdaWVNGbU96WE11L2FURnRPSi9LWWpJWlk0NXJKZDYwbXZkQXlXaXNhV3pGR3QwUnFEaVhRZ1pock8rSkdJNGtaY3NWV21CUzBZcHF4VVZrcEsvR3ZES2o2cHBVZTFIQVEyR1QrMDNQYWZQM21WRUZWUmFUSnNWYTVIZWRxZW1LRzFUSVo3MDBPTkpMSERWR29YM1dFYUduQWdHaDBjZ2lSQk80MEdCRUxlckZpbmVzdjRFeVZqZGUweVNKVmJYMFVOdjNsb3FKRGE1ZXhPanFlVkJkK3dZY0NDVElka2tDaTZGZXo3OHlhckRsNEJFTUIrc1I4aUpNNlFqbnYvYzQvdWlmL1ZQTVRnK3d2THdJNTV6WEJVek40S2QvNmIvR3BzTjNZUkdWUnpvMDFzYitBd3htNTNENyt4N0FpNDk4SFp2MzNvUXIxVFFlL3NYL0VuLzY2Lzh6RnE1Zjg5T0FnOG9tUzV1dC8yTjczbmtBUEkwZHI0UGc5RXZQNFBUVGoyRFAwVHR4NFBqZG1ONjZFeXZrYmNRMTBxVHExWis1djU0U3R4M2ZXSXVOWmpNRlNVTStiVVZPOFptSFFNQkpnTlFGSWJNOG8rVVIxUm9XWTJjTGNIZ3RUZTVOeG5OUzgwR1ljRVlhWDJDTTg1UnR6NGkwdHZlZDdFRHR0Q010ZkF1aXlwVFVLeEpkeUZLOHRKN2dQdmtQLzV0ZmJVOW1UVDE0QVZVWEU2TTRpQ2dvYkZ3T2cwTWRFeHdJRlRFY0ZKVmo5TUhvTzBhdmN1Zzd3aFF6QmhWN1ZKOXo2dTg0TlByRXV4VjQvMGpUMjhhT05MbzhSWEU3dXluMzhWUEFEamd4RG5rb3FJTFFoRHBBTlV2cTFJZ3VVanVLYWpGTlVQTGRNWFZnUGtFU3BVZGNkQWhHTitYMHlreG9ybDdFbi83NnIyRjIvaHlHWjA1ai92VHJXSDduRFl6T25VWno4UjNRdFF0dzg1Y3hXTG1HVFRyRVU0OC9pbjIzSHNFS2ZFMCtmT2QxL002di9XTk1PZURxL0R4cVVUU3VqenMvOGtsODhoLzg1NWplZlFoRFZPV2dqUEI1UVF3aFlIYmc4TklUajJEZjRkc3hzMnNmTm16WmltWjVFUzg5L2gwY3V1MU85R2MzUThCcEtoRk1NVmFzR2NQdkY1SnpFV3pidUI3dTRsdjR6TU9md3VWWG44ZFQzL3dLU0JyTXptNEVPWmRjaFNuUEkyL1p5dnY3bFE2RW9BZEp1SFBSbW0wN2dYMzJDWUpwMHNFWXlyWHI4K3VGU1ZUT3daeVFjbnNRUG9DVDJxTEp0VFNFelJ1Zko3YnMwaGlic1M0dW9NV0JRZ0g0aGluWWtrYUVQTEJlTVlGZFp0bjZsZEVCcEZTSERMSWNFRmtYNlpiSU9ZZlQyKzg1bjJvN29wVHV4NzBXMzlRRk9xOGloaVAvWjBvYmg3S0NNQTN0OE5VNEkwc1dFMFZrek9DamJGTEI2V1R6WXFhUWtsY0VFZ1dIM0Z4QXFKVjhlUkNsdU14QlVjbGpYS0hqWk9RSUFIRmdITFJJaVRWSmN5bUJLNUpRV1RZdFFobExZQ0wwNTdiaG9iLzk5L0hzNTM4ZnUzYnR3U2lCUTZFMEl1ZU5Pd25ZczNNN1BuVDdJVHoxN1MvaDRBTVBRYmdIN1UzaFIvNytmd1lDc0hYUFhuQi9nQTJidDJKcTB4WXNTYmlyS25BY3dROU95SHdjRjd6anBsdlFtNXJHK1RkZngvNTczbzlscmZDK2h6Nk5FMDg5aHIvKzh6L0ZKLzZUZjRTckd0Vm5RYXJiR3EyZVlPQmlZUVlna3htOWpWdHhVWHU0ZlBVS0huandBWHpvd3cvZzVHc244ZFZ2ZkE3WCtyUFlmZVFPVEcvZWdibzN3QWpPbHc1aHdJalhsNVdURTBpcEhMVFJ0aDIzeWtSNzJpZGdUc0x6SzNOZkF1ZnhSTkNrVXhrRHJSV0QxeG01bVk0S2dVQ3A3cU1KN1VaYU9GTnBTa2M2NXJOcTdlUXprQmMxTG1Bdno0b0JubFhobk0rQ0tpSlRndnNNdVdMQWZlb2Yvc3F2eHZIZlRQbTAxSUFZK2g0RVNkVXJSK0NPQTNMUDhLZDgvTjB4K3F6b08wci8xZy9aZ1dNQ00rQ1FUMG8yZjBhSC9Zd3RsQXh0SlU0K0dJU0hLRWdaUWR4c0hGUGVSUDE0WFVDRDBETmdERWRURmhLMnAyOGxvdVJ4WGNwWDQwd0VUYTJyU0RWaGJzeGd1UEFnNG9ocVNwUlJ6RmFVSExiczJJbVZoVVdjUGZrS0JsTURRRGswYnVUWEV5VmNtNytHcWNFQWN4VndaWDRCYzd2MmdtWm1zZTNnWVd3OWRDdW10dTVDZjh0MjBOUTZDUGs2Mjk5dkRoeTJmMDMvZHdxc2kyTFRvSTlUMy9zdTVpK2R4ejMzM1lkZXI0ZXE2bUc2VWp6L25hOWh4ODdkbUZrL2c1NERYQ2pqSERRQWlUN0Y3NUhBYVlNS0RTcHQ0TFJHVHdVOUNDb2RvY2VDTGV1bjhmd1RqMkx6emwyNGNIVWU2emR1eFB2dmV3K09IOXFEK1ZNdjQzdVBmQXZOeWpMbU5zNmk2dmQ5MXVFNGdHcE5PSHpZc0NwR0dhZmpwR2xVS3ZvU2VCZHBQS05VMGV6TmtCRCtrSG5FNWh2Ly9MSVVuZ2pwc09zNFVNRm83NjFxazhhMElFZlRHV3QzcnhaaU5LV2ptckpMeC9pQkcxQ1JTYjJDTm16NnZuTVlWSVFCK3d5ODd4aDlSNkIvOXN3WlJkTEpaNVZVYlBhSktMUTNsNVRRUmhwcWV5SzR1RENRTTRFSXpDR3A5K0xmS1lCK2dSc21KQVZid29XcE5CM0paZ3R0bHcrRlJsV2RVREw4RkRHZFlJb2tHR29VR0lwZ3BSRTBRaGhLZ3pwUWFGTE1ZYVBVU0FUV1ZNK2xOcGFJSHdnVkhMMkdFNHNvaUVxRm9Dd1o4YlYwV2VUMDFUK2cyV1lKWC91OTN3TE5YOExVekFiL3ZyR1BPbEtXSWVpODkrNjdjUGJjQmN3ZXZ3OHpPL2FoYVFMYXJPRTZRbnUwYW9OR0pCaUROS2pyK0hjSjJuekJBRFZlZWZJN2VPR2JYNGFEb0pxZHc0R2p0MlBQZ1p2dzljOS9EczJWQzJqSW9WbzNpK201elRoODEzdFI5UWNKbjVHNmdUUWpieFJTMTk2c1pMVGlVOUZHd25IczI3bDVOSVNPbHZHcGgzOENUY0IrZWhXajd5cHMzN3dKcyt0bThOWmJiK0ZyMy9wcm5HMEcySDdrVGt6dDNBZHhnd0NFWmlZbFovUEcrSkl6bHFQVzRqdTJlSVpHR3U4VFFlVkFEb1AzbHlCZHFmU0tpWWVLQjYxanRrYXFoWVpmTmY3N09OdU1WaDlISVFpM2cxTkNXN3pwS1VnbldoQTFpbEdCRVdlS20xbDk5aDFLb0VIbDBDT2c1d0lXQjRBZCswejVONTQ5NjJVMUpxMG1JalJKWHlGbStFTklHNWhRc1VORjRTUWdSbzhJbGVQRVg2Y3Vwb0NmT1dMREZrUmFyMnQzbURFeTc0YnJUMWVUR3NQTXRRUDdSUjhkZ2lsVFRBbHRqLzMwQW93Z0dOV2hGSkFHVFRTTlVaK2FSNDIxS0xKd0o2SEJIZ3dVWkdlYUppSFlqTVpxSThLR05NUlRvaGdsZ0tTNWhaWFFKOEhzOEJyKzNmL3lxNWlkNmtHSlVEYzF0R25RUkw0OWlCWXE1N0J4NHl3dUx5NWpadE9XOUNIeWlXUlVsaUVRazUzWklKSk1MWWlCS3B3UXpBem5IS2FtcDBDa21KNmE5c0U4dE5ReWdPV1ZGZHgyN0JpbVo2YkF4SUYxaWMxcXZzdFNnNmxJby83UEN2SUJTQVJEVWRSS3Zob3grbmNSaGRRMTFrOFBzSzdmeDE5ODRZdDRZNG53c1ovL0w2RHJOb1orRG9ZMWhOVVdRNU9WbldhKy9GaG8yL1lzU0FuWUdhcE9rMmREYnFDSm5wTUtTYzFqN1hld0xialdBazhET0oyVmkycktLRGFEdzZ3b1NKTld4ZXFPcVRXajA3KzZSL1pkMkF1OWlvT3dEcGdPcDM2UC9XSGRjK0VnOWlVNmxjbzAweVZIUm5HbGtaYmoxQm9IamtJZTQyMldlMXJDNjRoUFliVmptMFdaZFNjVXd6eGhWR0hVbXZmYUpORXNaNU9KOExCRVMrZVhLS0FSVlRRQWF2WHR0LzdrOWIrck1naVYzOERoZTlPSnJxWEJoL2ZRODU4eFhWUHN3b3BxcXhnNDJJQkF4dUk4TDB4TmhveERNSWFER2R4NTN3L2kraHNuc0czWExvZ3E2dHE3K2tRRmx3QndsVVBQVlRnUTduM1RDSnFtTGsxWU5KejhRYkVvWWVQNzZUNkFTQk95QUlJSStXN0tXbEUzSTR4cTcveHpvYjRjdkFJMXRNSDZ3UGY2eVpPbVU0a1NoZWtURnM5aU5DS29neE9SdndZZkRCMHBkdTNlQzNKaDBqRG5NWEpOM2VEeTFTdkF1bG5jK2VBUDQ1NmJqMkVldldKb2FObjRvaTFVdlJ6VTRhbEcwNkt0R2FPSkpac0dyTVZxTmhCS3gzd3dTNTRBbEZnUGhwbXdHcFNZc2NjbGRUU0Y1ckxjUmw0cW9DbGx3YkN0Q0tvRkZjbW1oVmMwM0ROdFl3eVNTMnNvcW9yUloxK1c5eHd3NVZ4UzJmWkNDYy9zZjY4cXl2QVVOTFM3SnZrbUYrMXA3ZDUrRGN0ZEZCaUZGTlJTTFhiV1FSUWZ4QVlXWjVueHN1K21HRENwWkFZT0I5MXplM0I0RXRWcXk0RW5ldDRGVEVOQWFURkh1dERydXFYbDVTY0FPYTlpaTk1Q0VZQUp5d1FFb0lraUhUSHkwRnhHT2VNM0VMdk5ZaVlVYTlIa2ZTUU5CcFhESzIrL2cwdVhycUJ1YW94R0l6U2pHcUpOZGtNT0gxUVVFUEtaUjlNMG9TUmlNRGtRTzdCelhpWmRWYUdudndKVi91dnNHSTRyTUJ6NnZSNnVuMzBiOHhmT1lHWm1CazBqMkhYck1XeTg2WURIWG9nOGZlUXEzemprWFBpdmdxdjhuNGtZekI3eklHWnYxOGJPMys5d245ZkpJajc3ei85MzdKQW10SDJ6THgxR1E1dzdkd0hUTy9iajdrLy9MTFlldkJYTDFNTVN1U2lSd0hobHZHRUZnbnRPN2k0TzZqcW9NUTRwcmJkZ3FPVU1Nbk9tMUFLaXIrTUc4S2drZkNvRitQakp4Tk50cE5rbklvdTIxS2dFTlRuQ0tQbU9RREtxVHpKZ3FzYlc4eWo5NStoTElBR3NETDB5QUNwSDZETjUxaTFpY3NTb1dGRzVMTDZMaDNCVkVhTUo5WkVZaDU3TVgxSUNKbUxnYVVKckxvVWViRW44ZjNjK1lieVpFbHg3b3RzSkdWa2xDc01HTTBoVFlzQlFBOUZrbWprV0V0SFJOa21TTmFQd2pXUktLUFdDSjdWT2s2WUJKV3FGckg5NzRkMWNHRitvalZCdGg3a2dja25TVndwd3BScW40QkQxbUFna0l5eGRPSTJ2L3RsbnNXdnJacENPQWtqVGd3VFRqZ2dxT1NhOGMvWWNidi93SjNIWGh6K09JVVg3bFBnOURHTFArMGZYV1orTnhNMmNXM09GZ0I0Ulh2cnE1L0hHRTkvQy9mZStCMXUzYmNVN2JoTTJIN2tiUS9FMFhvMnNuWWlpM2NpRFIrMUg0ZVlUbm1FZE5vZlRHbWUvOXlqV1RVOWhXRGR3b2hpdUxPSGN4Y3ZZZDhkNzhaRWYremxzMkxFWEMzQzRwbDVNcEdJSFZ5SzB6aXBhTUZGdUFVNEthMzhpVzJZN0tRSEpTcTY1OEdJZ2E2aVJzanRxbFJPbUgyT3NTREczOFZxUkVZelVsMkpYcUVxU0FTWE5EZFMwK09aR2NTR2pVbzNXZFdIenEvb1NuQlFnQi9UWWwrT0RLTGlEb3NmcWRUZWM5MmdNaXRWVTVWQUxZUVJGQTBHajRSYUpHa3VzVUZOSFdneUNPZ0JyTE93NTkrUnNVM1k2WmNFQ3R4UlFOaFZDWWgyMDQ0dVE4UUlOd3lweWx5TkJVaUVoWFJ0UFJWQXhtRWdzWXZxaXk1N0ZPUGdpTmZIazBaSGUxeTNjZUlrK2hqR2pNWlkwa1hlMWVXUWVXKzRmWGtNS0NuVWZRN0JKVi9BSHYvT2IyTE56TzliTnJBOHBaY0F3UkUxN2M0TnJWNjVnMitFN2NjOG5Ic2F5NjZNT2dVVkVJV0doa1JFamxXUTFRQkpUVTcra1Jxd1lDc0RPWWNQR2pkaXphd2ZlUER2Q2lBakM1b25HVWlDV1J5UkpJRVhrMVpya0N6TW8xSXQ4d3B0T3lRaFBmT1VMMkxaeEk1WVhGM0htL0FYY2ZQK0QrTVJQZndRejIvZGdSUm5YTkFQREVPbllmeXVQc2ZIV0xQaEpuYnBxeERDcXhRbE9DZGtKdWF2YW1wOWFFbVR6ZThKMVVMZ1V0Vyt1dGpzTGlva3duSTIyMVhzMzJPTGIraDRRQlE5QjV0VHh4MW9HRjBxSzhzaXNCWW92WkFCZWIrTlFPYzhHVkp4UC9jaGlnUmpWVkFVTUcxL1RhT00zakxkMUluOUNwcmJLREdJMnh0eWhJUUUxK1Q2eEdGZmJvZ0dpS2FXanFYK2dIYWtOOWhCZGE3MjR1UWdJUXBtdVREcHdvN2xYMnlOcTllbG1VR08zMzh2OHJsS0drNkJ3RTgyMlRRS0xQQnQ1Y2l3N1RJUXY0S3ZRVTY0QXByWEdvMy94SjNDTDE5Q2YzWWlSd0hPM0hJd2lJcUxYQ0JibUY3SG9wdkhEUC9VWlhFTVBGUWhUV3VQaXFWY3hXRGVENmJrZEdLTEtKNy9obDZNSXhSdXArdGQwWU4va2s0RFNCcU9nU2tReWFNNU5WWnJxM3lCSDlsQXhIQWpUampCb2xuSGg5Q21zakdwczJIOFlEU3BBQk1zWHp1RGl5VmV4c0c0ZGJ2dmd4M0QvMy9zZzNOeDJqTURlVnpBMnhjVDZqcW5JQ3Uwd21HS2tabXpIVFFZK2JWK0ZscmxHNExiSXJHY2ZiOVJNY05aVSt6dHdrc0ZuUXh0YmZFcFJtbkRTSFdyN1E0UnQxK1FwMTRwQzFWb01rZFg4R2F4eE1HbnVhRldWd2gvRE1kQVBZSHpmK2Q5N2dZVnpiRE51NDhRRVJUVkY3UFVERUJDYzd3UG5iTHdaalN2SWRMQXBPSE1RTmxVbXo3RmJVMFdsQ1RNSFZFdXBaWUV2aUhIQk1SdkgrMFA1MmpxRWZaWElOR2p5Zjh1eVpTcVBQNDNHRXJFK3kyWWR5ZVRCem5ocWFUYmlhUnlWZmlMV3RNUElRemdMWnFpNGh0WTlHQzdoOUF0UDRlWHZmQm03ZDJ4SG96VmtWQU0xQVZ5bG1yWlI3OVYvNXVvOGZ1S1gvenZVTTV1Q2JabWducitDMTcvMlp6aCsrekY4OTJ0dlk5ZXh1N0Q5eUhIVTFWUUJxcktpTU02azREV3NISUlsa2FjTzZ4RWE0WURVeC91cXFZMWJUTXR0OU5Ucmp4Wnc4dkZIOGRwVGorS3V3NGZ3K2x0bmNQZFAzUVR0VlNCU1hMNCtqeC84OGIrTHcvZmNDNTNaaEdXdGNyblpOR09NV25NOW5pSnJjc25sL0psU1FJZ0hnWFZ4c3ZyVkFNNVNrL1BUMk1vY2RDRFNNV0poSDk0U3lzN21QWEpidW1rVUwzMG5JaDBaRTkrZ2Qwd0hRbFN1YXBPTlJZMWxYbVdjeWRUWWZIQXhSdERMbVd5UFRUK1VBSTRKamtMcW40Qkk2bWdscXA3ei9seXF3WCtOR2RwSUdzTVZKY0haZmptYkVDcWFqSGdTWlJlbUNMWkZ4WkoyZGM4ZGtGRFVTR2twbmFwNVZDTmx0OTVBSFNXUE9ja3FydHcwbDJ2dHlCWTRzQWZQV2hONEZVRWxGSnB6b21OUnhQUTVQZ2ppb0s1VE9BMTF0U3FhWXJLci96eGNPTjdZanBkd2NvaGkrY3BsL1Budi95NEdxSER5NG5XNDNnRGtIRnl2aDE2L2g1NGJwQ20vVXpQcjhSTS8rMUc0clhzd0JDY2dxUm1Oc0dGMlBRN3MzNGU3Nzd3RGI3OXpCczk4NzBsc08vNCtDUGNNUjEyZVZTcVNuaXVuSWFhVVdJUW1kUjF5U3V3cHRGVTNZWE9KZWlIUXk0OTlHeGVlZVFUYk5zNWk4OXhHTEs2TWt2cE0yR0g3NGVQWWVmZ09qRUNRUkhrRlppSGM1ZmpuMU9tcHZsY2p1ekVGdXRlczMveGMxTFJ6bSt3ODZmN3RDZXZDL0lndy96QWFhQmk5QnFDWmYxY2QwOU1STTcxc0tFTnhqeGkySUdjYW1nSW1jZFNRaHEwb25GMmxGRUgwWm94VWpBR3FHdGJBejhWUU9KQS85Y2tMZTNvdVpBRHMvVFFpNk1tMkZkQmsyNVZ6b1NKU2gwWUZ2ZkN3aDhuSFBnZ1Npb2sybW5YYVVkR2lzUVU3cDBwaW1oU2twYnBHb05KeTgwS281TVc0OTFyTWx4aE4wRHFMTm9sWE5UYWVPWENJSmd0eVArL092MGFkYnJKclJhRjJlNjlYN2NVYXNlYVFHY1RJQVBqQkcwRWd4YkRhZzN5bE9xYkQwS3E3MW0zZWhwLzd4Ly9FYzdjUnVZOTBLblB3UDR6Y05tTkZnQ2FLdG9KSG80cW40SzdNeitQSzFTczR1SDgvbm43N0JDQ1M3Y3ROODQ2MFBBMVM5Nlp6bnFFd0RTMStPR293ZGpIb2EvWmk4T3pGYUdFQi9VRVB5Nk1SbHBhSGFBTE5HTjJFZlRzMkZRZEtacmMwTlZEbHprek5XYUxaQVA2YUpXVUprckpMQjFIeDNaOVJERVhXUHlDcTk5aFFQUVljSnFzdHlITUdLR0JJSExNUjQzL0I2VE8zT3IyamRiaTVWN0ZyTm1KZGJMb1NpUTIzcjJMazVYbDlTZ0xFTmVNQXFvSEs5b0cyNzd5alZvOEFSNUxXVFZ0N0ZJTkx6TkFyU3ZTVTEraHJCSGxZVWF2a2s1OUtOOEFJTnFWd3k0RkQ1M0E2TTZlejI0dnFKUFRlWnlNT29nVHZsVGI5c2JZUFQ1RkQ2UkZwTlNFSFJyYW1LallhMldieGdNSlNOQUdWZ3BheDgvNDBjRTVrcmM2aWo1Qm03WHVhK3hMNVdKSWN6eldhVzVMUmJHdkhNU2M5Rkhab3BtWURrUkJOTGVQSllQWkMxRGdZUGx6dEdQSElQTGdlS3VkdzdleHA3SnkvaE43RzdXakFhUXlZRnNOTGNqTG9pTDNyY3FEeXVJRER3bmM1ZnpyR0ppMEhCWTJHV0x4OEJoZmVPSUVOek9ES29XNzhjRklKOTU2c09pL3kyaDFGcEJYQVNPalJqOVFzSjhlZjVJREVVUllydWFaR0JDbURud0tTZWJReENPWGs4Ui94b2VUUmFDMnpqYWRlRkczNWtrNExxM0VZdWk3TmdvaGZDNTRZdGtEa1pDcExSbnBmR1UrQUt0cjYrdXlPOGhBWmlhS1JrTEVoQkpWZTRQc3JpcjAyUVF4R0NsWnFUZkhTenF6Qkt0cDFPZldnZ1VBZ1FjZExBbkMwMW9vM0pMd29odzJldG1Jb0dVUVZ3b3BHRlkyRzlFcERaY1BHd2tpcDArWk1vVDJVS0hlV1pjY2ZvN0tERVhVVU05d3RIV1lXTVduU0hjYklucEkrcGFJL1hGR1l4V1FVUDFsTmNlcno5N2RKVERCQnBzbUkwNkdSZXVXTjVvRVQ5NHhXRmtLRm9VMVdrZVhUSjcxdXFNbWJ4c3Q4d2NDMTY5Zndrei84SUU2ZGZoWFBQUFoxck45N0NMdHVPUUpNejJKWkFXV1hwS1JFREljbTJXWEhrcXBpcnlKck5QdllFNEFlQ2FhcFFUMS9GVys5OGpKT3ZmZ3NzRFNQelZNOU5QVW9NS3RlRENTaVJxdWFCVG5XVnp1WmJkcjJZbVRVdndSb0tVUGZRUGxueEprVWJEb3VCYVhsZlpsOTViSklUYWxXem5Dd2pUcEpnQlB4SnJZTWxlYjJzSEFmWGRUUkJENlRvd0VPTXlxTm1VSHNOcVFBK1BzU0phbFhtZnpFSmpEcXh0dllwWE9SdkxlRHAvZEN6YzhVZ25rVUpHVTRpOVZVNkFiYnF1THQ0TkRjMHcrcENOZmV5a3RjVll4VWNpR2xyQ0xTSEJEVUpnem1FRlUwb3FnRGl0dkVHNmd4MVpOd2dad0dYNVF5emJoWnM5TnJLaEVLVDNPMjJGNWhENVcxMjVSTUVESXN5S2FMRUMxamhuS2lhTlI1a0trbDQvZW00UmZHbVRhWE5wb0diWVlaUlVsU3BiQVRhYWdjL1V4dFdhdjlIdU1pRTRNWSszdFQxN1dmNlVmQXhjdFhjTzdNV2V6YXN4dWYvdkQ5V0ZwZXdxc3ZQWUx2djNNSlcyKytEUnYzSHdUNk13Q3hsNFZTYUFtVktOMVZPTWVvaUNGb1FDcHcwcUM1ZmhsblRyNkNVeTg4QTE2Wngrek1ORFpWak9zUVhMMThIUnMyYmdBVXFNVlBJMDROWEVZZ0ZlYzB3Rnp2UkN0YXhiZ2hsQk4rY1lHMnQ2M0FpbmNaTzNVenUyS3JkbWNWNWlhdlRCOXJ3VWVRbFNVRm53dE9nU08yeS92bUhFbzlNOXlXYjZmUUZWcldGV2lVL1g0S3dVQjhHZ0ZGY010bVJjL2xybHNLaDVMdkJqU2VTS1JGa2h3UHVTcCtmRlkyaG91TXF0TFF6NkhCVHN1RElsNU00RThJQkp0ckpVQ0NVR01rQWNUMkR0Q29oWHdwd1prSWs5QmpvR3BTc1FKSE13OUJjL1lCbzlxeXJzUTBaaTVYY3BVeFFhTTBwRVF4d29tMUhEeEo5clR0R0htVzhtUk5FbDh1ZXJUWkRJelFwSG1RUkZjV1ovNFlOeHY3YjZRbC94ejEvRlhWeDhXTGx6RmFYb0dyR0F0TmcrKzk5aWErODl6M3NiNENiamw0QUFmMjdzV1AzdmNEdUhMMUtsNzQxdWR4bWFleDc4aHhiTnk5RjFPVncrS2w4emh6NWgwODlzaWoyTFoxTTZadk9vNzFXTUd3WHNFN3I3Mk1FMDgvZ1lXemIyRHp4ZzNZME90alJScWNPM01HVnhkWHNPUEFRUXlZUVUwTlZ4SE9uejJETjg5ZHdVMGN3NTZNYllHMWpqbGpoNGl1eVIwVHBTZytQamNxM2RyMFhiM01KRThzQStDMmxHRnNXdFNUa0VjRmpuMi9SQzhvOFhvaEdGU2hIVGNxOHBncGk0R0NtYWxLeUFCVUlBelVqYUJtVDhISEVXVWNqWE45NzZtZm5SRktqTTZrc0xhbUxXYWszemg5UlQwU0dWdG13N0R0Y0UxaXFBK3Y1dzVPTDJ6YU05UnY1Z1lDRVVLdERXb0ZocXBZcVFXTkFyVmtKRnpNWnRSQUhuQWF6NVJUZXdXTURxQjBLTXJaR0JtamprbVByVzI0SkxETnZXUUNnSXlmVDEyOHQ0U1VpclJVTUNMUWt5UjVHRU5TRHJZbTNGTG5CSkt4MWs1S3BjRm9jblpWQVRHajN3engxbk5QNElWSHZvNU5BOEt1WGJ2UTIzRVF0OXo3Zmx5N2NCNXZ2UFFDWG56czIxaXZJeHc0c0J1MzMzNEhObS9laklYRlJiend5dXM0ZjNVQnAxLzlQZzRkdkFsSER4N0E3Y2VPNEhPZi8wc3N1SFU0Ly9hYjJEUTk1UUcrcFNWY3VuSUZWNjh2WU1QdUE3anovUi9FM2x1UFlkM3NCcno4NkxmdzVGOStEc0lWZHQxeUZEZmZmUjltRGh5QmNzOHdxcEpMT1MxUit3a2VXTy9TRkpPTVdhMjUxM2FzMkx2K1JhWFlKMW1CWjJyVXQ1R0pHUTdpczB3WE9tWjdRWC9mWTBLZkNJNmRQNjNKbTl2R3ptK096aGdhaDZLeUgrOGUydGNia1pBUlNORGhVT2p4OTRkeGp4a3VBSUZNRVgvaFpLcUR0bk5DUEZDL2VmcXFScEZFZkFoTkdQUXBodHBTNDdHZWJMeGlKUnRHRGpVSU5Zc0lHaWlHRGJBaWlycFJqTVIzdHNXaDQ1bjJ5WFZORkhvZzFLU2wwUkVWanJMalJNZXdKdFdhYVVKSjUzNW9jalpEVENrMTVWQnVSQ0prNHd5YlVTQURkYXlsMGl2TGt2UGlpRlBJU3ZXSVRqaGg3SkdvcVV3WTUxU0xCSUJ5UXVKN3FMRjg1U0t1WHJ5SXVWMzcwZCswT1RSakNxcG1pS3RuM3NMSkY1N0ZHODkvRnh2N3dNRUQrM0RrOEdGY25aL0h0eDU1QWx1MmJzR2hmWHR3LzN2dXdsLys1UmR4K2ZvUzZxYkdwWXVYY09IU0pmUTJiY1hSZXgvQW5zTjNZTEIxTzBhQjBxcVlnSVdyV0w1OENadTI3NFQwcHJCWUs1cGtDS3FUOXZka1UwdWJveGFxdTlYMy9qZ0hqWFlBb0padXIzdGN0RS84MGdjaTlyVEU4c1RMMENWY0s0UFFnTW41N2p0SDZETzhOcDhZSE9pNUt1cnhneTdYdFdkNXg2bmJnV29WSmQvSUZZZmloa001SHNHVml3Q2dwZ09hRFp0QzRBQW9heWNWcUxUd1ZlT01Nc2JVSmpZRktJY1phcUZOa2tQWFdVRENZeXJ1QW9BbjZwMkRxcURzMHNBOVM2SGxwMFNicEMwV3N3NXE5MmdGQldLNjBjWnBSYnVKT1FxcFpUZzVLVHNZazVuem81YjZpYUJWNm1qVWNvNWcrck8xUkhlQjZNci9udXk3cmQxdHE0d2dNeWN4K3JwbkdpaURmd290REN0aWZ6c2xQTUpoR1FSczJvblpqVHNoeEZnZTFvazFZWElZN0x3SmQremFqM3MrL0JEbXo3Nkp0MS80THY3dDcvMEIrcjBlcGpmTVFrUWdUUWd2SW5qajVFa3NqNGE0OC8wZndnZU8zNE9wSFh1eElBNGpKU3lyQytKTzhSalgxQVlNZG0zRUFnSGFjQnJ0VFhHVXZMSGhUcTNjSGFGVlZ2YTFhNktzWXFORXRScEJKWXBSaVpROUc2aFZTbWlCRGRCWTV6MktveVBUYUxkU3NNYXdIYTlOWVNOUGdYNWtEcVB0Z2hxdkYzd3pLbVBTVWFoZEM3Mkk1SzdaSkZKU1VOQWxhbXBocCtDMTRGR3RORkExc2ZLU0QxUW9wQkhUOUpTdnVZb3oyWE5xazVkL1ZoMUphZnRNaGo4T2l2WklWNEdqQzdEeEU0Z05HbUZhVHlHbkxidzRLUStYTkVDTEMzOXpFVm0zR21KdGVmQmJYd09ZeWJLQnUwbXo1MURLVEdPUGc1cnhVS1NjRmh6SDBXSGFhcGhTTlUwZm10WXVoMUxCTmdERjlKUTFNaE50SjVrU0NDanc2elRSTnFyZWtndXFqL3JrQUduZ2lGQTMvZ1NXS0p0V3dGR0RxZ0pHeTR1NGZQNGMzbjdyTkFiVFUzanI5TnU0ZGRPbU1BbW9RVjJQc0dQSGRweTdjQUVIYnptTXl5dUtwWVhyR05SRFZOVTBSZ0pqNEtLb0ZSaXdCZ2Rudi9wRWNpUVRMcWY3VWd1SnMrS2s5SDFxaHFPa1RNNXcyR0VPb2tzWWc4L0tkdHdBQUNBQVNVUkJWSUhsSWl0ZEtPMGxEeCsyMUdTcTRlMFFscXkxNXp5M09SalFkZ2QzK3FmbUFqM3AxNmh2eUFFR2pqQnd3UWJQeGMzUFljT3lDZVoyZUc3T2R3c1NuL05JTUlGcnphYjBhekJhOG1teWo0K2lQQWxOWURtSUppbXdOOUFPdTVJb2lEN1VpSDNNQ0VyUzBoamRpRHVzVVh2a1NSMk0wU2I3S0JWbDV0RkJweEVKR1FQU1prdDFvb0hzSkkzVzRpUzI2UXhwVTNTR2MrUm9KNjJXWnJ2UThrYTFGbkxSRHBWajg0OFo5ZVFDTng4SE95RHgvUTdGY0V5MlRTdmFLbGVUUTJBdU1JeFRjWjVURU82THhrN0dVS1l3d05wZ21nbk40alc4OWVyTE9QSHMwN2oxUGUvSDNNSGJQTXBMZ3I0TWNlSGtxM2ppcVVkeC91VExtTnV3SG9OK0gxTzlBWmdkaGl0RG5IbjdiU3pPWDhXNjZSNTI3dDZMTC96VjEzRG84RkhjZC94bURJY3JPUG5ZbDNCbVdiSDlsdHN3dTNNZlJxNlBHZ1JHamJlZWV4cFgzM2tEQjI0OWltMTdEMEFHNjdHaTdOV1lKdGkzK0RWUUFhcEdXM1VmSUl1S1ZjMGtKREw1a0phVGliUDhuRHFjdnAwSGtCOUQwbmliQTdCMGgrWVVsS3lUa0dRUUxXVElIRkQzS3RUNlhwampnYmtxTk90RTBWaWM2c3ZKSUNRelU5VDJCQ1prVTFzMURza3BmRWdnSE0xZ1grTWNSa1h2aTJHVFFvQ2dMNTI2b3NtTm9CaXNIdXNKbTQwYlkwNmJRb1htRjVnVEorWU5vZ2oxQzdLYmp0aVIxajRScnNPMDM5Z0JWNXUwUENyRk1sYmhOZUhlekNPN3ZNUU5sVTA5eWhtZllyckZzbmRiemplaWtJbUVEQTBueGpOT3pFWU80aUpqQXF5cUxjVlVaaS9FVkY4Y3hTMUJ0Y2JKVU1Jb0U2MVd3ZGcvcWVGeG1CMEd6VEtlKy9KL3dDdVBmeHR6RzllajMrOWg2NUc3Y2RzUGZneVh6cHpHcWVlZnhpdmZmUlFiZW96Wjllc3hxbXRjdkhRWkM0M2lqbnZmajlIS0VoNzc2bCtoSW1EVGhobk1ySnZHL0ZEd3dVODlqQ3ZYcnVIMHk5L0R6azNyY2V6b0VlemN2Z1VMQ3dzNGNlb2RyRXpQWWNlaEk1amJ0aFV2UGZJTkhOMnhBWDEyT0huNmJjeXYyNHFiNy84b1ZyUm5RR1MxbVg3THl6K1hhdzI4dkZXaWNpT09JRXNORzVwQm8zTFl6c1JhUGs2QkptMlZIMW9pdkFveGJFSU00QnIzZnQ1UUtSc0xFNGRaVUNGYTVIazlmci9pMEpLYlUzOE9ERnNLSG1ZOGVucTJIU2dqSDFRQ3RNclhMSDZ5M3g5TGFlcDZqU2N4WGd3SzFVaEROREFnbWgxRG5PZVN4MWVnN2x5TUNJMWJGWi94c1l1QVJMVHIxbURCWFFVWnE0UXhYdUtiM2lBdW16Rks2UHdUYlZuUlN5dEZRbDRvSFBOdnpxYy9CYXFFdURzRklkMXc1cUl0MmFzZE9kdUdLeVVCalNyWkFiZkptRUhKMWdlNTluVUZMNU5UWkk1bFRUU2tLSUN3dkJqWW5HWWErdGdkSzVyNWVUejkxYi9FTFljT3Bxemg5QXRQNCtUM25rRjk3UkxXejB4ajA2REM1Y3VYOGVhWmM5aDc3RTdjOS9DbnNlZVd3NmpXellEckZTd3VMdUx5cVJNNGR2aFc3Tm05RTY4dEtBNTk0SWZRS0hEblJ4N0N1Wk92NHZHbkhzWDhONytEdlR1MjRMWmpSN0IxeTFaY3UvWUczbno1U1Z4NjR4U3VUeC9GOVBRQUR6NXdINzc5K0xQUTBSRG85Vkp5eG0zVTM1ak1xT2xYWUVVQmNLSEZ4MmZlM3hyQTJuU3JQYnpVZHRocE9SWXN6UTBzNXcwVzdYb1MxWUtTTlJndXk0bGpINFZqRGhaYlFNV01IamdNcCtHUXRRVDlDMGwyRW00N1hLTTlzRFl5RGRreWlBb0d4UURWTVRmUmNtNlZxR1Q4S094TE93cXZHaldTVXB6c2hCSUVEVnJXcEJ4Ykd0T2VZek1WUmhKcm9PYm9qWUNLbmJhU1ZFa1VXNVVraVNIeUZKTXc3UWM1V2tyOGpMRTNPa2JBOExPY2xITklHejMxNTl0T1g0S3hJcWVDenhVeVk1WUZ5ZGdEWmdhQmFEa0l0TlB0UjlUbHZaVW1vTjdkbVV6VWFXbTFCQ1pTZXpXMFF0UFVYbkFsSHV4elRGZ2Z6Rkd2RU9IVW0yOWhidDlCSFAva1I3SG4xdHN3UGJjVkRmZXdRb1FWSlV3TnByRjkvMEVzbm4wREIyN2FoNXNPN01mQ1pXQVJEbzBBNk0xZzl1YmI4TUROdDZHWnY0cTNYM2tCWDNuaUVkUlh6K1B3ellkdy9JNDc4SjQ3aitIcS9BS3V6YzlqWVhFaDJLaUw2WWRBYTQ1MlYybUhJZ1ZHeHdBMjFaV3R0dDBTeHBOUU5nV01valFEU0wwR0NXSGliSWFMMk5obVdRZVNRcUJtQktiRjNBa1hKTGhGeW0vVXN2RXBTOUxtNTQ0Uk80SEhhajZTQWxOSzNZcDJ2SkdvMkcveEh0bVNndUxlaEhjcXN2ZTNXaEZ0K2EzbFFva3BjOTFhYUpTaTQ0cWtpNUNpLzl5WWFocnV1bGpNZ2U4WGl5VW9Tcjh6eXB0SGN3TjFOTzFPNnFsU1dhSkdCV2lxcGFEVFJyQlBpcU9wTXJacHhsUnJhK2F0b2ZEV1FtZXR4anRiV2lvQlhHdWt1c2tzR2cwZG1NbGdUTHlEYkZNM09ILzJITnpzSEc1LzhJZngwY08zWTJyVFZneXBRZ1BDU2dpR1RpbDUrekFSbkt1U3cwOGpRTjFZUVUwUHl3VFE3QmJzdlB2OTJQY0Q5MkhoOGdXOGZlSUYvTWxYdm9YTlU0eTc3N2tIS29xbWJrS0RrcVI3ejhiV0hhM3JKOU0zMFEwS3JZQTY0YjRxSlgybndZT2kwQ2FMa1pTNGNQMUp1YjI5dzFIZ0Zob0pvalcrNWZqalBXT2xaSk5QeG5PdkY2bHlNbDRTWm40bVdWT3o2RlFjOFRQTjhtQVJHeHpOM3BKcytwb0cyK1Fwb0MwSHBMaExVeEdSOVRWUVZDdE5ibSswZ2dHaVBHUXozcWZDYkRQU2QwRzFsT2JGZ1Fwdjh6d1VNWHFlNWU0NUcrVWpYcEQ1VDByMVk3b0pNYldQcDNCYVBHeHU1QmhacWZFMkxHSkw4aktRUW5hYUc1STBxeFhiYXFBYnFzckdMR0xWenI5TjJ2eXJCUnJWbHNZK0tzY3dBbFJ4NE01NzhNQi8vRE5ZcEI2R3lsaldtSGtaQUNrMG1zU0pQc1FNY3B4QUxVOEh4c3pJdDg5NlkxWEdrQnhvYmdmMjM3Y0R4KzU3QU4vNW85L0Y0dUlTWEkvUmhORm1CUTdTY1ljeVFYYk0xOVpDK1hlL0prWVZTb2xhaGdpSWcxdVNjZXJOazUrQzhvNjBSUnRuclQ2YnJyNVlxckVwelRnMElGV1VSOTJUWmNwaU9hMmxwN0U5WnBKTW5kU1k2TVlRM1FEa2NzY3JSVzhLU2kzNktYdTNsbnJJcnNQeEFKV2toZ2xCYXlpTmo1YnB0RFpBU0lnYXlSelV1cHRvVHJ1aXVVS1N6MUpHcTdXd3pQQis5Vnc2c3lXaFRtcUMwT3kwYW1lNXFXWTcwUmpwMU9Ea2F0eUlZNVMyR0lGcXArcFBSS1BZaktXdEFETzFBLzFOTmo4d1Vjd3lwbGhvaGF4SnZuUGhWT1BjTmkzQjc3OC91d1h6NkdNa21jZ3l2U0Y1aUFwNUN6Z1IzeUhIRkUxRmJVcE9pZjZ5OXZSRWpJWUlRMGNZekc1Q1hkZFFlRVpCalNXM0hmUzUydjFacStKUFRlUzBScCtGdTVQbThmU3hhYWNLdTVLUkc3RlNqenhSUVVFbVhYN291WSt6THBPekRzZVRPbkJSNFh2UzMrUDNhU2s1RjIxcHZneUxsTEdxdHVBOE0waWF4bUNiVE1kbzZCTlludGFxSmtBd2VXSUdaKzNvWlZHcEpHdVBYRjhuc3daL2N6aTRsclFmVWtiUHpXZ3NJcENJcndGalY2Rm1EbE03eTEyTGR0cG8vT0R6SEUxdGwxQXFJbDVLODhtSVRkSVVYNE1ZRnpJeExoZ0FEUThQYXAxa2pKTnNhNnozYXJyeUFzd3lvNSsxNWFIUVh1ZTZxaUtPREdaaFpoR3FGbDJSVElSUlBjU1Z5NWR4ZFg0Qmh6NzRJeGhGYjBOeVdTaGxaMHNFWDM0SkxkL1dITlBia1JuL3VGQWFKQ2RuaW9NMlBITXp1MlU3bnZyT1grSFFnWDNZc21FV2pVaTIyRks3WWJuVmg3SDZ5WDRqVFhZY3hHS0ZSdEZHamRQYURjTnFDTWxmSWMya0lOdUFGU3pDTmMrZzVHQzFGYiszSWs3YUdESlczeExhNlNtOXA3U0FQQlJ6STlXTUp4QTdmandQVEU4clZPM0FrTlJnWmRKOXRkQ2xGbGlMZ2pQM3J5WGlsQXhCeEZLQW1pa0VhM2ZVdExKZnU2UUxIajMydTJ0b3I1WENKS2RRN1VSa1dBbzVUUG4vbW9aaVdQdE9MVlJiZHM0RHRWcDl1bWRxQkM0bEsvbTBPMSt1SUZKTlJOWlZrdElVSEtsVmFoaHJwdzVOcFRveHp5ODk0cEF6SzNNWEZBS3ErdERwT1N6d05JNS83RUhzdnVVb3ByZnVSaE1MTGMwTnNubmwyZkdlRkJwTW1qQkxJR0FDUVdlZ2twMmdtR0Q2T1h3R01STENnYnZ2eDg1OUIzSHV0ZS9qaTQ4K2c4VlJqWWMrNk1wMlg1UFdkaHBUU1F1UmpSVzVXR2JLS3Y3YXBBcWl0ajVjcTR2QmtmUEl1NGpJVTV4aEdmMEFXdmhLNU5RUkFrd01IR3dtWFVrSXFnNkVCazBBenpXMWZMZUhkNGgxRmxReVE4dXo3RWpUWkFrcHBnMUpuTlNWc3ZNc0hDSmg0MGRneWdrZDMxaW1yZFN6VW5XbFdBUHQ3anNVNDdFS0YwODdvVFQ1T0V1WWRKSTkxRkpLcGdIMEN6UldvK08yUFJrMW9xUUlhVnQ0aVZxVnN1YmhpNXJNSjlSNkYyVnhiMUQ0ZWFNRkh3eVN1U2VydDJqaXJNa203YmFVMlNhVlBBaTB4UzFEeDZTMkpVZE4xUGxLV2lTcFRMTDlCaHBMcnZDZG9tajY2L0IzZnVWL2dPc05VQWZuT1NFMlppMmFxSlgwbVpHdHZHTzkycWdmNWxFM05ZaDZmczZCb3V6Q1ZKU21sdXF4ZzJVRmFPc3U3Tm0yRXdmZTl5R01SaXNZY1pXVm8wak9IQ1luS2x1NHlXYUNzRzYrSnF5bkdDSFdWaUE1UWNRNWt4VzdNUHpGMDNBY1RuL0hlUU5INXlVcWxuUW9DOGhtV0p5SG1CaW5JUzVtV1ZpUVVzMWFESWFyeWdhZjV2VDhRc0ZiQ3VzTWJxSm1CcVZTeVpaRUlLK2NRMnA4Q2EwKzBuWTVxdkZMQUZCcGtCcHBHalRBUllUUlRFR0crcHBhT0J1WnhnaFRvNW9zZ1pRejRwdTY1bEFNU09BNGZVVnRwTS9qdXRTa2J5bGVTdzVZSlBrbWtTMFZxSjJXWjdjZ0NpVUtoNFdjTnF1WWRKTmdRQzFiQnVYQkUyQTFHQjlsVnlFMTVZdHFwM1BGTkVpblRXSnRuN0w2cE4wNFM0bUhGaUpJYnhyRGVBSkZtYTRZVlp1ZmU5VGl4NUVjZW1KSG1vcWY0aE52V2FDL1BWc1RnMkVVekVSUHhwRHVLd2hEVlF3QmFEVUlCcFptODBlSnJXcUxNa1VCemlWemxUamhLTlRmYWswNDRqd0ZqYlc4My94K0dyVjZteXdYVzI4cEdXUlFWT05STnB5TEJYOWlsTmlJTDZKOWw5bllSZERpZ0t3TG1STFphMGVVbXJBKzJadnF0bVlWSmpNWTVKSGlta0E4eXIwc1VXcHU2d2JOSFNzcHN5QXpoVWhiN2Mxa1dZNVlRWVJHSW52VHpaYk4zWERHSnRsNDg1Z1VsNHE2UTlLMEg2dlVJb3ZWRk1LZG9tMFRlYm91a0NPMG1vMUFhcWV1NWxGZi92L0NadU1ZSGFtd0NMUE9QbzVENjNGa0ZjSUp5MXdLaE5RS1A1QnZxSXBrKzJya2RGR1Z1Mm05RVZRbEt5a3o3elRGbThnN216NEJUWStaQzlVZ2s4V1BrYnhzTlN3d1RpS1hETkRDWmh4RVlZaWszekJhTjFpNFBvK1ZwU0drY1NCcGtIMzB0Qmhta3Q4bjQwV0Y4NDZoL1NpS3I1UzZIWCt4akVuajV5bjU2YVZzTjh3d0lMWEROalVkSHRFVDM0K2doL2ZDRHlkczVjSWdUTXJOTjJ4OEtaaGFWSXdHS1RkbHo3bzB6azFid2pOem1KV0hqRkdFUnIxMmp0ZW1IYnFzMytNK0ZJU1pDekZBSTlPUnFwYjF5djBZZHIxSXNQS25BRzViaWxWTWhoWHZmMFhXU3E5SXNkVjB2Vkhxb1U4THFwUXRtYlFtby9reE1tdEtwVFZIUGlWcmRsNmNxcFNNR20yR0lkbE9PYkFKY2Z3Mk1hZWdSQ3dKeUFFYk96T2p4WThLUXlCRzhSaVlKZmM5ak9IMXJSQ0RnbVVWdWJLL1hRM3prWTFOVEdpdGNxa2xSbWZSYVZnMTA1V1Q3Smh5dWx0WVpRR29iQjlCTG9ZS0p4NXlQaFd0U05HRG9MNStCUzgvLzEwOC91VXZvamRjd0t1dnZZcG5uM3NHbXc4Y1JYL2RETGJ1MlEvdFRXRkVEcUl1alZlelEzUEkySEpsOVdmMGZ0UkVuK1hMNCtBTFdEWnVSUisvZUEvaVJpMTB1V1RtUHdSLy95Z2NpeTQ3L25kdnhGRUZxNjJVN3NlbUxqdHExREJidWZNeUc3aGtRVDRaR3EvdDMyQ24rOFpneDhYc3lOeWg2QThvaVI2RHlWRXFNRjhhaDhZSWxBUk5VTmF5c3NrUXJHbXE2UjVJMXQrYWNreHFEVHlPSFk1ZTlpeW9vc3BOcVcyTmdwYWNNcC8rdGpNNlo3dUVNcmtNdFpwMlFkekNncWxJdVZIWVFwY3VibFNpcXJCMWNha1RVVEtOU1NpSFNtcFlsR0ltOVVSdnZielJOQStCaEpTOGZZRURvQnp3V0hnV3RQRUNMYTYvYkpBeHJiL0dodHArYjZMeVZEdHdwZHJhdXRDM204VUNRT29SK3MweXpyOStBaTgrL2lpZStjNDNVRUd3Ym1xQXVjMGJjZVR3clRoeXk4MTQ3YlhYOGRLWC94alAwd0Q3anQySm5iY2N4ZFNXSFJpaUY0RGJGalVzeG40TjVHY05LQ1VhVmx1ZGEzYkNEbW1uSTdnN0FNaHc4N2tyMG4rdkMrL2p3b1FjRG9HZ2lpV0NacE5PSlY4TWNTQjlOWTF0TjJhZUJ0eW1lRkRrVVZGK2pZUVRXbHV6TFVSTHd4ZXhSckR0Y1hKaFFsU1c1MGJoaGFIOGhJM0VQWHhlRHBvSGlXc3RUbnRXQXpvYnArT294T1dZY1FSY0xSeGFGUlh0VWUzTlBvNmMwUklETEF4R0tDa0k0ODFTV3c2RE9vWU5sRENJZkZZWG1VWHhQdEpxRlM4eFRqVTkyM0dqa0hJNVZUYmhmUzE1TXBrSW45YTFyRW1Tb2xwYWxuUzFDMlV3b0VJVmFUemwxRFlHdDh3c3FmUXIwREd3WWxaL05Sa3g0TGlTRytES09mekp2LzFOdlBqb2R6QzdiZ1liWnpkZ01EM2wrVzduQUZXc241bkM4VHVQNHE2N2p1UGlwY3Q0NmFXWDhjUWZQWUxOaDI3RGJSLzhZZFNEOVNCeVJjT1ZraFRYUTRvVVhEdnRMVW9kMFU0R3Jib21IWm1PamhKejR4YXMyWmI5LytIc3piOHR1ODd5M0dmTzFlem03Tk0zVlhXcUw2bEtLcFY2T2JhTWpZMEZob0JqbWdBaERZU2IzSkJBeHMyL2tadU1PRnlJblFTSXdVNXdCemF4TVE0WURMaVRHNEZreVM0MVZWTDFmWFBhM2UrOTFwcmYvV0YxYzY2OVM4Nkl4bUFNWkVtbnpqbDdyVG0vNW4yZlY5bUhSRjRoSTVVRUtsM09SWE5tZFdhZlRTVzNxdFRwVzZFMm9pM0hkMkVPS3hxSkxMVFRaRGtHNVdkc3hQMTVjZ2RzY1ZFWlplM3d5NTJpYUdzSVhLUkxaM1dIcVJCUXhIMnZjbzFHN3NqTEEwZnl6WUlxK0J3ZXlvQmY3aFFyZ3l0Ny8yVGIwbHhQcCtQTFY1YkpSUnhic1o1SWVKMVlLT2JpRkhsVGhZMzF0WEVNSGNYdEsvWmFSMWxLdy9JbUtORlUxakdVNXlCWThBMVZCWVZXZE5qS0VReFhBSjhULzZVdE1YWlQ3UlcySGx5VjFGL3I5aFJMcUZJQVN5MCtzamlWR2FYR1hRRW1ZWFRuT3AvNmpYL0g3bzJyck8vZFN6ME15NXNsaXdGUGtqZ2wwTVFSUnNjc0xyVDRvWGUvZzdjTngzenhTMS9tcFM5OWpzZmYrek5FNFF3MjMxeE5JTmZLNzljVmpZbDEzTG5MVlZ0OXFhcUFuS3k4RlJKTEFabTNVYW5DMFloZ2pNTG9sR2dsOW00ZTEwRG9GTzFXWXErOWJYTEFuMVpJTGthS3RDREhzVlZjUUhtV1pMYlNFK01xLzVTbEhoRGx6TDZLMXRpUVpqN2Fyc1JNMVVnbVVISXE0V0w5V1pyZmRCNzBvMG9qVXhGTGw1UzhDZC81YlU3VG4xWit5S3FJUjAzc3h5dkFoR0oxZFM5dG03MmVVbFAwY1htWlpYRFNXNVFsZWxBR3hPUE5BWE01anJ0Q2o3S0NJMHlGMzUvMmZSVTFvWk1nTTMzWDZsWUlhcm9mSHRmR2FWT0ZjeWNreWwwejVWSnFsMjhzanZSYTV5S2Z6SWlpSlNIYXVzTW4vdU8vcFhQN0dtdDcxbEJhWjJiSnNsczA5dGJCZ2hGRzQ0Z3c4SGovKzk3TEYvN1huL1BpWDN5V1IzN2twNUJhazFJeko4N1MxcDRKVmIyWHl2b0UxSlNLeW1Vc1ltMllMQ2ljWGIxbENkTEdnTkZDbEhNYXNpZ3pzcG1CM2ZPTC9hV3ovMTZKbXNpRktBTmlTeXUrTy9BczhYQTJNazhzK2I0b05Xbjl5cDJKVWxHQ1d0eE1zWWFOQlRVbmZ6NFNjVjJPbmtKYllSdjVXRTFiUTBadFA0ZFNwanI3N3RNcjFnMXFUK1RmUkxkbVcxL3RsMVhsYlBic1liWkZEUGM4YmFvaG5mWlFURTlXRDZyeWlkcEphdGFHSVRkWktHZlE2SUlYeXRmSGVvanQwRUJIaW51dlNxVlNFZG53RVYza2lrLzRLdXp0aTJNeUZEWDFZSEhBTEJoWEJwdFArRDJOVGlKazV6YWYrczEvUjMvek5tdDc5MllPTlZXRXRoUTVxQ2hIUDU0UzN3Mll0Q3J3UEovMy9kZ1A4L2svL1hOZStPSWY4ZmlQL1gzaStpeGFhOFRFN3UrN2lCbXpaamd5MmY2OXVTYlN2bmlNcFJSVnhRMnNjMjZBTVJpdGlVdzJETTBHaUxHbE9VRkszNHJvY3ZaVEtFM3R4YlhnTWlQeWlzVUNBNmk4cDdhVWNHSkh6ZGtYQ1dYQWFHR2J6OTJHWWd1MEhPZFprUktjQzRsMGR1QXByWndEVW1kTWpSSm5VSkt1QzF5ZnhVRFFLcXNtRk5rUThFMXNLS3A2K2RzQ2pYeXFuL2N3MnQzdENoVWp6YlJpZXFLMWNPVzdkdjdhdEh0MnFuTGVYU3hVNnBSSzE2d3FQYlR6UjhqMEIxVktGRlVaRWptWmVKTStFRllhcmJ3WkJ0Y2FxbFgydU9LTVdNMWtQZXN3a2RNSHdqY3hxclBKeHo3NEgramV2c0hlOWIyRk1rK2tqTURNc3h1MU5jOVBwK1k2dTJCTjluREhhT0RIMy9zZS92aC8vUVhmKzlMbmVPeEhmNGFvMWl5Q0tncFloUzBlS21ZcitRR1hURG9nYk5CRFVka1pseHlTNGRrS2tiMVFxRWh6SjZwS1ZCcmpsdUFnMVpTdG5NdzMycllmcEJKRmozWGJsNStNc2VZd0tRaTNJQlFwbHpBbzFzRWhZc3U3c0MydjFqa2pGWEdiRkprYjRneVI4ME1nY1JEZnVhcXg1SFprb0J1UnlVMWJubDZWSDB1L2MvcU9GSU1FdTQrc0FoVHRrNkNZTUx2ek9HVXRvWXdTUzhldXJLUmFVRk1zNGlKU1NZaXhkNzZsQ1VLVW1pRHBUYTVpS3JkTTFraHBhNlZwY0xaWHFSdk9HQ3NUMEtMN3VvMk44MUJoNi85MUtVNXlCRW01cVVycGUxaGRyYjJ4RFJNVnUxTVd5N0JpYkhkQldVNExlSUdQTmdtNnM4WEhmK1AvWmVQYUpkYldWcTBJY0dPRkR5bE1FakhxOXlFZXNiWTR6NU5QUE1xSkJ4OWtaVzJWc0ZaTHg0bFM3cStSaFBFbzRrKysrQ1gwMmpFZSs5R2ZKZzZheGVUTjJEdzdTNHNoeG1yWFlLTFNzMWZMT1k5Q2xMTEtZWGQvWGVoU3RGdlMydm9UVmEzYW5EK3ZyQlR2NmNvVXF5aTJMajRxLzIzK1BkcnIzTUxBWXcrREN4Q29jUWJYZHRzamtpTDNpd1BEOW9OVVdDVU8wY3FxT1BQdlFZdXIyQzJOUmJrdVJLSCsyOHQzeGIzczhoOU9UNnkwMHBkVVQ2UjZGVU1SUzlKcGx6WGx3S1d5eHBNS3VhVU1NQzk3WWlOdXNUQzEvTFpmbW9ySFc5azY2YXdFcy9oK3hRTnFpWEowL3BLSlZlNWJVYy9pU0lHcmtuN2xDS1JNTWNGVmhYakVPZmtyT1gzNXoyNFpDS3hmVC83d2xMK3I4alpMZnd1ZUVyek9GcC82MEgvZzd1V0xyS3d1Rnl0RXJSV1N4SXdHZmZxOVBzUE9McDJkVFlqSHRHbys2MHNMQko2aTArL1RXdDdEUTQ4OXdaSGp4MWhmMzgvYy9EeWU3NmN2dU5MMCswUCs5RXRmcG5IZ0FVNDk4ejRpdjFIR3A2dFNyWmpieW9zekxoOTZLVmVpVFRiaEYrVmwwQXF2OEs5UHpJV01wZXEwTWhwTEExSzZxcXRxTDZ0dTduTDI3UXFVN1ViR0tPVklhbTFMV1pIVG9HU3F2YlBxOUN4QnRjNXF4L252UzVLdnJRTFAydzZwaEh2WVEyUlZEZ3dMZTQ5eTA0MXpZcGJrNjFDRit2RExkeVh2WGFxREc4dTFNK1dscTJDTW1MYnZwK2pIa0ZKVkpwVnlTZGtyRHlXdVpsd21iNHVTMURMNUs4N1VRS1VtVVNSVDY1akszRjNLNFc3MkpCaGJjeURpM3M2MldxeklMYkRhOFBMcUtxa050cTY3a0dYS1BjWWVwY3hUTERoRXZ2NHFady9HN1JXRkhGbUxtQmkvdjhPblAvUUJibHc0eTk2MXRYUnZIMGRFd3dHOXpnNmRyVTNHZ3k1YWhMbFdrL25aV1dabm1zU2pJZXNyODd6cjdVK3l2YlBMbFp1M09YL2xLcmZ1YkdEdzJYUG9QaDU3OGltT1AzQS9hM3YzTU5OcTBSK00rT0pmZnAzdzRIRk92ZlB2TXZScmxvZGRyTEJsVlNHanUreEVXN3FDdFI5d3g3WEtMUnV0alVxeHMvRnl0YVU0bjBYeGtpckxLRk5wckNxVHdmTFBWem1DM2RwbTJHMnJzdng3dWVNeGUyZmNvWE1sbHNkK3Zxd1Rxdmg1UkZkZVFXM04wYXBEUlR1QVVweG50VlJteWtUekxBclVoMSs1STlpU3hLcmQwcDVMS0d2NnJhbzhkYW1ZdU1SVnRVMEZhVlJ1dnludWVMRytoaDMyZ1Robk1YWk1WMzY3YTlGRjcxWmt5ZVBNNlNyNmVweEVXeksydW5MRVJaV1FTNnZxcVA2SXlvSDdsZFZPUVJLZXdFTGc1QkE0YmpqN0p6VVdteHhKNDg1TmpOZmY1bzkvK3pjNS85THpMQzh0TUI0TTZleHVrZ3o3NkNTaVh2TUpsR0pwY1lIWlZncy9oK3ByNkxVNzdGbWE0VWZlK1JRbWlkUGtZaEU2L1FFYmQzYzVlLzRxbDY3ZElGYUsxc0lLRHp6eUNJODk4U1NObVhsZWVQa01DOGNlNGI0ZmVJYXhGNlpBaThMRU1zVkk1WFRKOW54QUhJQ0dxeHUyWXVCc3F0Q0V4MW94UVI4dDFJbTZPbURCZ2NoYlZZcUxMcmZiNHNxVjV3dzNwYmpvU3FLelRPcWZjenRPUHVCVTlqd3FqK3BURnBRMm16VVZmRUltOCtTczMxUE9BVlEyUnEyWVk3Z1BxYThzanFLZHBscEpGY1p1NG0xN1o1bSthMDJ6VFNYTHk4b2p0bFZnVE5nNzNlVENQRmZBcUhLS24wOTB4WmJhaW5LUEMyVlBqOTFabm5MR0daSVZFeFhSVVg0YUs0bzEwcVJqV0Z3OWt0aVRibkgzeEhsTFlIRDlCUFltd3JwVjhnUmorNE1WVzk1YnFXVTlFeE1NZC9uOFIvNExyM3o5cnpIeGtON3RxelI4eFZ5cnlkTGVSZVphc3d6NkEycUJvbEVMMDd4SFkrVXdaS0dtUmE4ZlIzaEttSzk1ek85ZjVPaitGYzVkMnNQTS9CSzM3dHpseXBudjh0bVgvZ2JSTldwelM4emR2RUUwN3ZISUQvMEVQVjNINk93bXRwMSsrVTZhY3Q2UncxM0xuMHVYbjZselJxcEtIMjJQVHBVejBDbnNWV0k1S3RTVURBYjdvQkJ4ZzV0RUt0ZVVLd2gwRko3aWlwbU1NbWlUdHRIcE81Y1A4MHBscXRqYkd5bTNHbUpGNXVVR1c1TjdJd29EbkJSYkNZMjIrTGo1b0ZBNzhtTW5JVWxLOFpwU2drOWlEZDBLMjZseWZmZFRPMjVyYUdNUHBjVTE4SlN0VHZuRHBpRUYxdGN3MXMxclpLSWsxdlliYkVWNjVhZXZRMTVSS1lmQVRuRVZWUTdNN0gydnNxYWlaY3BRZnZ1b3l1bXVIT2x4cVprUzU1YlBUMml4eXc3cllWT1Z0V1FaV21ZTkh5c1dXT2NFTTNsNW1hQzFSeUFSeWRaTlB2K3hEL1BpWC93eEFjTHl3aHp6c3l2TTFHc0VmanJOYjdjNzFFS2ZSaGhrdkw3S2VrM3lPR3RLc0dzQm1GUm9sWERrd0JxWHJ0L2lMWThlNDIyUG42RFRIM1ByN2pZWEwxL2p6cmtYK2M2Rmw5bTllWU4zL3V3dk1tck9FMlZWUk9GNEZidDZ3N3J4N2VySzdZY25obkxPR2V4V0FpcUxZMDB0QW1MSmpkVkVtMXJsTmlyTHdGOFNCcTJEaTJsQllsVVp1UFZaWmJNUDhjcG5YOW1HUGt0dVJxNU1yUlFYaFlrcFAwUXE0M0h0Yklpc29sRVppOEdJWStyS1pZN0ZFUEIzVDk4Vld3R2xGVzhxZ0hVVVo0SVR3WFVQckVWNXkxbHRnc014c1RMb0ZYYU01TDMyeEcvK0o2cnYrOTE4bjM5ZldSTGZxa2JLaU9zN3NOOVRwUng1TDVab1JWWFdmeTRJVTV3VUV4c1dLdFk2U3l4aWpVaEVpNWh6ejMrRFQzL29BNHp1M21UdjhnS3J5L1A0MnVMT0dXRnJwME05REptYmE2YllDZXYzbmErUjJ1MDJxd3N6dlBjSG44VEU0Mnp1a09rNXN2N1RBTDNobU9zM05uajQ1QU5vTDlYZkc5RjBlMlBPdkhHQjE4NWRKanh3a3ZmODRxOHlmK3doK3VJN1ErR2lOSFVjREdLMVM5K2ZEbFFrV2p2TnJEZ3ZBRmxVZlpIeEx2a0xNOWxhVlRiY0dXdS9ZakFwOTFEM2dNTzQwN05pZDVNZlFGWmJVYzB4dUxmc2Z2SkpMb29rbWN4V0ZTejB1YW8rOVJvWG9wY2RSaC8rM3QyaUlwK1U2U2lMNzFaZHA5eExyU3NWYzQ2eVhwVEt5MkFiWS9KZHFrZzVlUlVwRkV6bGlWdjJOVFpWdHR4ZjV6cDg0dzZPc3E5cFJOekp2Vkwza0tWVWFZTk0zRkJNaUdETEdjVTB4cDJ5K2tmYlRtMERRTVN1d0NSbC9oV3JQcTFUL253eUp0cTZ5VmMvL1RHKytkbVBNVmNQT0hKZ0gvVXdTTG1BcWdTbmJtNjM4WlZpWWE2RjU1VlIxTVZCbkxuMmRyZDMyYmM4eHpQdmZBS0pSeVZYVWV0aUhab3UrVHphTXpQWWh3QUFJQUJKUkVGVXZSSGJPeDFPM0g4a2l3VExEaVpQYy9YbUp0LzhtNWRvUno2bmZ1SWZjZXFaOThIc0NsRXU0aEczaHN4Sk9uWktrN0pYZHBWK25Da0tFRnRESlpVV1NTcjdaNVhkNzFZajRyd214cm1KYlJlcFZOQmQ0dWdaeTI5VHBnbmRuV2RRSmtiWTkwNjJWbFZGcWRQMFZOZWEyYitkOFNueW9KMWM4RFNSaDZKQWZmaDdHNktVVEl6a3BJaExtaXgzbE1YUnU3ZEZocWszb0RQMnN6OWNHN3R0YWEwZHQ5ZzlHaEo3RlZKdVZxUU1FN0g1OU5qaUd1dndtWWFoY3Z6YnJuRkQ2UkxiUFBsOFdwNXRhNk9uclAyN1dMdGp5Y3hJOXMwamxxUzNtT2FLb1M1amJyenlBbi93b1E4d3VubUpsWVVXKzlaVzAxbUpLUS9yeE1EV1RnZEJzZEJxNHFXSkpuZ1pLQ00yaHVGNHpLRFh4eVJwUU55QnZjc2NPYmhHcDlOaFpXbUJ4YmtXOVVZdEs2d05XbnZwM3RuejJHcjM2WFc3SER0NkNDVkpzZklUcmVrTkk1Ny96cXRjdm42WDhOQkR2UHNmL3d0YWh4OWtxTUtpYkMwK1M4RzVIWE1rV2RWRVZiMmxSZExrS3Ftb1ZtME5TRTYvRlVmeUxhZzN5U2VRNm1ESCtYeVZXN1ZZWDhtSVhma1pTOGhEMXBOTDRhRjB6cldzN1N4dDFPVldJMmREcGUyZWRsNWlseDZvaXNwRks0M1c1Y3pDbURKRXRaeFhXNS9CaDA5dlNqNkFjZnB5UjNldENrbGl1VnVmRkhHSW8wWXFTeW5iNERKaHJWVWxzTERhaTZYZnFMRU1OcmFDMEMyaWNwcHhYcTZLMkVJS00xMithNHNrN0svdjZFYkUycmYvNzhFckhlNC9aYmlJa0ZqQ0RWVmwvRmdQc3ltOC9mbEQ2eXRCZDdiNTZtYy95ZGMrODFIcWtuRDR3RjdtV3pPWjNpQXBpTXBpRkx2dEhuRWl6TTNPRkx1U0tFNFlEZ2RFb3hGYUsycjFrSmxtZzhBUDZmVjZMTFpxdlB2cHg3aDY3UnFkZG8vdVlJVHlGR3NyeXl3dHpyRXcxNklXK2lrNTJQTzRlV3NERWVIQStocEtUQmErcVFybTNZWExOM251cFZmb3FnYW5mdndYZU9KSDNzK3dOb3RSZnZrcTJhUmN3Zkd4aTdFdm5iS0N3MzZoVldtb3llVzVVb1RTbG1HeXh0NzdWNFEzRkhaZ1hlb0ZiWHk0b21CYWlEMUFGbDFJM3NXSzliYXpTNG9ETFg4SHRDbzEvODZhMkQ2Z3l1cklaSU84QWdxdTB2bVBXQm9MVnkyWTA0ckZxVTd5bjFIRUJaR3JENS9PS3dBM0tDRmYvenVYdFgzbk92bm8xdDg3ZzFWeGJMaE9LMkRiYys5UjVZblRINWNVbU93M1dZSTJNblJUWHQ3bm00MGNkMjBjdFpXeWlDejVHajNmSFZ1NHN4eG9xWlV6eE11L1ZyWDFrSW5JYTdIbXdsSkNIYkh5Rm9wNXNEVnh6ZzQ5bGVrTnROTFVaTVRkTjA3ekJ4LzhqMnllZjRYbHVSa09yZS9CODZTMGtsb2RYcnZkSjQ0TnpVYUQ0V2pJZURnbWl1TlVEYWxoZVdHZU1FeFRoTkxLdzdEYjZURFhxUEcrOTd3VkU0K0pvb1RCYUV4dk9LVGJIZER1ZGhtT3hpd3ZMYkc4dk1EUy9Eek5ab3ZMMTI2bWdhSXI4MEJjVHBpem5PcE9iOHl6ejUvbTltYVB4dEZIK09GZitsYzBEeHhucklOQ0ZWdGNFaUx1NzNGQzVsRytORTdGVk5DZXlvMkpBaXZuSXFQdFdGa1NudGFZSkNrOERLSWtjdzltUWJYaWFrMHlaWFNtdVhmTlNscFpua3g3U0YwRWZCaTNmYmJub0NaRjB4V3hjRms3WkpCc2xxT0xhc2ZZQ1VtVmJaM0pvK255RlNOSnlnd293bEhLelpYSkhJZWlURG9FZFBlYVdBR0RPUkxhRlVGbzY1TXhZZ3V5VktXc0tsY2QrV2tHVTBDRnZIbWFEbFoxTWUzK2RRaThsVW5wdlpEZVJTdWlTa2E2SFIxbFJDYm1HSk45NnIzL01rVlFhZnFuZVVwblpaNnAyR0ZWZG5Pcm9sUklmejhKUG9JLzdQTHRML3dSZi9yUkQ5RWs0ZEQrUFN3dnpEbnB5Ym1qZXpRZTArdVA2SFo2MUdwcHVWMExmUUkvUUdtUDNYYUg1ZVU1ZkswY2VJcUkwRzUzbVoycDhmNW4zb3FLaHdWY1ZXc055c09ndUhMakRzUGhtTURYOUhycElUTXpPMGVuM2VhQkU4ZFltSzJuWk40OFdrN3J6QkhuOGVxNXE3end5aHRFNFN3LzhBOStoZVB2K0JFR1hvMUV2RUtpTGRabXdsNjF1VEp4NjZHWGttZVkwV2FMMjlDclFGWGxIa1BnNnZpdWJJZFZoWFpWcGxkWDVZUTVaTlNPS0hmU052TXF3RXFGdHFFNFJ0TG5BeXV3WERJUEJoYVNUYVo4ejdxQXhLUjhSNU94QUZTaFVqUVdFY3VxMG5Pd3lrZGUzbkRqMnl4aWJJNWZzbHNEc2ZwNWxRVVZHRWRUVlZWd1ZkU0RWcWxpUXo3ZmRINWZvZUM0UVk2V0lNOXBMNVFyN3JJT25oeEhucVBKUlNhUjRyazNXNHBFRnV0Qm5ONzRUM2orN1JXbHFxNlBNbitBRkYvZFEzbnBlTXBUUXN1RHJmT3Y4c2tQZm9CcnA1OWpzZG5nMFA2OTFFS3YwTCtKQ01QeGlHNXZ5S0EvUkN0RnZWYWpYdk9wQndGKzRCVXYwZDNOSFdiblpnZ0MzeDB1WlEvcmJxZkhYRFBnSjU5NUdwSitRU2hLc1Z2cDE5Rit3T25YM21EZm5qM01OaHNNK2oxMmV6MTIyejNhN1M2SVlYRjVnVDByS3l6T3Q2ajVQaUp4OXBKNmJIYUdmT3Y1bDJrUFlsWWVlUWZ2L3ZsZnhsODl4TkI0eEE2WnVncnlFQ2RKT0Y4QmlyTEZhZmxucE1zTFNlN3ROWjJXSEtFc2NWKytKMDhrdDFqajBKcHNRWm9kUUZQTUd5eXlVQTdsb0FqWXM0MG9taElncGh5YWIza0lNc1gvVW01QnBEcFFMTGJTeWpuUVZHWHVKUUxxbzlZQmtLZi9PZkRQQXJRaHhZdGVpcEhzbUtQcXRGODdZUmkybXJoYUFVelQvem1yTW1jSW9MN1BJdEE5NlZYeGE3WnhGR1gvNlFRNDVMOXN4ZFNGNGZmWmY1UUhuSVZCVjBxY3JZY29YSm1yc3RZMFdvRVpNeXN4My8zeW4vRkhIL29BM3FqRCtwNFZsaGZuRUFOUlloZ01oM1M3ZlNTSjhZT0FlcjFPUGZBSmZCL1BVOW5BcnpSV2JXN3QwR3pPRUFhZUU5TnR3eW5iM1Q1ekRZLzNQL05XSkJxaVBULzkzcldYY2Z3MFNtdGlBeSsvK2hvUFAzaUNlcGhDSjBkUlRMODNZTGZUWmJ2ZHB0TWRFa2N4OC9OejdGbGJZbmxoamthemxyWkJLdURNK2N1Y2VlTXk0L29pVC8vOC84M2h0N3lUTm1HcWFNekZMQUltazlVcVpiRVByR0RZNGlhemZQbW95WGhKWjZrczVWQ3NWSEJMR2RDaHl1ZEM0d0pWamVWWVZCUFBvVXc4Z0ZKQVZDbFJSYzR3MFEwQVV4YldtNG51eDZacTU4WGk1Q0xRZnRMdHEwWlZGYWY1UC8vSXl4dnkvYmJxNWVtWUIybVVWQlJsK2I2TjdZUkNLbk1EK2Q4cW5TZlhJSk4wSEhEUjBpNVRmcHJFdUhJczJEM1lQVWcyV0s1Q2V3MGt0ajNhT3NqS1U3UXNFWE0wbHFxNENjWHExQ1FMcndBSVNlaGNQOC9uUC9KYlhIanV5elFEajBQcisvQTBkSHQ5dXQwK0JxRmVyeGNpSDkvejA5V2VLaitqZ215dE5MdWRMcDduTWROc2tKaDhXSmdGdDJoTlloS0dveEdkZHBkOXkvTzg5NTJQSXlhbTBhaG5ESDN0b0JlVTFneUdNVys4OFFZUG56eEI0QmxNWXJLZ1YwT2NDUDNCa0c1dnlFNjNSNjgzb0Q4Y1VHODAyTCsrbHozTHk4ek56N0cxM2VXNTUxOWlxeCt4OXZpN2VNZlAvQ0pxY1M4RDQxa3ZTSjR4VVc1emJPdUZMYUFwM1hLWjQxTDBwS096MUNRNUJDSzdmZFJWaDBFaFN0TkYrVittWG1vTDZTN09kVndLay9LS01ydnRGVTQ0cDdLbHNLanFsNWtRcUZLNTNGUVdFNmVxei9LRU9tajZCYVkrbWgwQUUwWDdsSXJjTFpjS1JNR2tybDVjcjRRNHEwVnRrYnJMRVppYXdFR3B5ZzhyYnlvQ0tyWDB4cVcrRk9rcTAxYy85MHFsdFRjZ3lpS3ZLaWtwKzNZM0lESWQxYW1uSEVPMkdDUGRnY2ZVa3lGbnYvMFYvdWkzZmdONjI4elVRbVpiTGFKeE9zRHpnSldWeGV4bDEwVUtUaUdneXVLdjhtL05VNXJ1WUVRU3g4elB0ekRHa0lnbU1qR2pVVXgvT0tMZjd4UEhNYlZhRFMyd2QyV09KeDY2bjRzWHp0TnNOVmxaV1dKbGNZSFdURFBGUnl0SmN3U1VacmZkNThxMWE1dzhmZ1Fsc1lVb3MxZHhpbEVVTVlvU2VvTXhuVTZYM2QwT3ZoK3l0clpNYTI2ZWN4ZXVjdjMyQmxGemhYZi9rMTlsejhOdm9hL3F4V3hKU2RXaEp5NVlMTlBMbHNvMyt3bTFDaUdtbDI5U2RXTllFV1ppbGVaTXRBMVNHTGVVa3gwcGxySytZa212TUtBbTMxUTF3WjBRaDRZdHBWTVVWUUdveWoyZXhUZC9vZFh2bmQ0UUp6UUROd3FydkxOMDVlVjNzOHlVcGVLZFdQZmxheERyZU12WExrVXlpbldpT3QrcXNsZVNsZEpIVllSTFVxRVlTVmw2Q1paY1YxVlNqbTNXdThpa1V0Q3lKMCt4ZVJTUG1qMEJGa2M5bHBlUHhva3RWd2cxRE5IZHEvenhSLzRycjMvcnkraGt6T0pzaS9uV0RMVmF5SGdjc2JQYlpuM1BDbHBMc2U4dFBRVmxZRWN4N0VVemptS0d3eEVMODdORVNjSmdPS2JiRzlEcDl2QURuMXF0UnFNV0VBUWVRUkRRNnc1b05UeCs3RjF2cGR2ZVlidlRaWGUzeTZBL1FDbFlYbGxtWlhtSnBZVVd6VnFJVW5EdHhoMTJkblo0NE5oK1VIR3h0YkR1cDFKTXBBTVNNWXdqNGR6RnF4Z0RmczFqZTJ1SG5kMHUvV0hFV0ljY2ZlZFA4SmIzL1FMSjdBcVI4Z3BKZHdFSFJTYTlHMVRCblhZQW1hc3FWZmQ2TlpRcXRqSEtDdUVveS8reW5UQ1NBemVNS3dlV2NnQmNFWkE3cmFCVXNPMjJOcjhjSE9vUzcxYXBlRXZVamtKWGJuNVZYZG5odmhOSzNKZzk5YnVuTnlVdmM0VHFQcjdzWmQxT1AzdVlLMVNmMHZkUENjWlVLdFdlWjJzN3lmZW5HVEpjVFJrQ2lKMktNZzBSSjdhVDNuRVRaZitienFhZlVyNFlZaFZCMXJDekNLVndoa2JweEZibkt6bUxZMkdLVkZrcFJFNlN0eWNXRkNYWERlU0o1bWsvbUJRL1p4aDRCUEdJQzMvN0xKLys0TC9IN054aHJ0Vmt6L0l5dGRCREthSGI3YlBUMldYZm5qMW9LZGVGOWtha1BLRFNBOXBUUHVNNFpuTnptekFNR1krSFdhWmNRQlRITEM3T0VubysybFA0dmxmTVpuWjJ1clFhUGovNXcwK2pKQWF0aVdNWURrWnM3M2E1ZE9VNnpaa0d3MEdYWnJQQndzSThpd3Z6M04zWW9obDZIRnhmUWF0czI1SC9IclIyVjZzb2xQWXcrSngrNVN3TGk0dXNMUzh3SEF6WjJtbHovdkoxdHJzRDlQSWgzdlZQZm8zVkJ4NW42TmVJb2xKZlgwSTdkZmFobEFWNWFac29zd1p0eExyZGhvcnpjbVk3ZSt2aVVCYWowT1FUY3lkZjBicUpzL1pIS1ZVcHhXMGNseFFPUDJOSmNndkprcFFXOWZTNjhFb2RvYmFDU1ZST0k4cTRGVmF5bE5qaUpEdGlYSmt5a3MvWklTalU3NzJ5S2FWRVZoVW5qckplNU5LUFhuRUg1b204ZHZTVk5pNVhzRkxwRkZBSXErUXZkdXRZZkRwYmtLUnd0Z2RsY1ZaV0c3bXFTbXk2cTVhTW4zNFBmYlcxUTllRjZNZzZ3TFN0V010ZWRtVU4rNHBmdExHS3U2eGZOSm5wS1VuM3ZNVUt5UmdDU1VpMmIvR1hmL2o3UFArRlR6UGpLL2J0V1dHdTFVempxTFJpdHplazErbXlkODlLWVFXVmdpRmY5cVFLalJFWVJqSER3WWhCcnc5QXJSWlNDMzNxWVEwLzlObmVhYk80TUkvdjV3SXQ1YXc1ZDlzZFd2V0E5ei96TnBSRXhiUmFvMEg3WEw1K0MrMzdyQzNQczcyMXpjYm1ObmUzdGhtUEk1U0Nnd2NQc0dkbG5ybVpCbUVZRkptREU2UTA1YUdWUnl6QzZWZGVaMmFteWJHRGU0dXNoc3MzNzNMNjFYTU1xSEhpbVovbWtSLzVLWWFOUll6bk85SG9TclRiVkJWdXk1TFNMTmFLMm43RzNMRlpEdlBVYmlPckt0KzRybHlzeFpsa2g3Q3FNa1hMV2d2bUx0emlDVkVsNnR6T2x5aFJjNjd6S1AxNXl4Q1V2QTBvcmRXS1F2dUxjcTM0bGlsTkZXMjRWMFN0cVkrOHNpblZVa0dSQjJqYWxsN0p2Z0UxQVRjb1h0Q2NRNWI5bWFhSTRDNmxyd1grMmxxeml0ajlGNWhjQWFZc2E3RmpnSkFzMjZYczA4dXZVOGFHRisvalZLaHhSZUdreWpKVDJWd0JLd2l6eUFvVVc1WnMrU1VLUG42Mnh5MTJzY1cveVF3eFY3NzN0M3pzTi80dC9adVhtRy9VT2JCdmxjRDNDL2wxdXplZzF4dHlZTjlhSVdZMXhUNDh2VlhIY2NKd05FNkhiSU1CV251RVFVQWo5R2swNnRRQ0h6LzAwR2p1Ym13ek45Y2tDSU0wYjAveW02RXMwVHVkSHJOMW41OTQ1bTBRajhvK1BoZTVlRDR2di9vNmh3L3NZN1paS3daK3ZmNlE3WGFIcmExZDJ0MHUydk5ZV1ZsaTM5NVZsdVpuVS95NFNGWXUyNGVuSVRHSzA2OWZZcVplNDc3RCs5REtvRHhOYjVqdzNkTnZjSHVyamJmdk9PLzZoLytLaFdNUDBSVVBJeXFMZE04T2UyV1p6RVNENWVYRHFaTXM0YzdFekV1c3BreUtGcldNMTNablUvbUZvN1BNeWp5eXZZQ3BJbmhaQUswRGhMSDVCZHEyZCtNZzRTZUlHR29LalRvUFVKM2FyMHE1b2l5NEFtWFFyUzJSVngrMURnQXFIZ3lxOUpCOE41NHI3cXlwcWhQZFhBeElyTDgzeWxtZkdOdHZZU2lpbXdwWnJtTENMZWkyZ0hiaUNtVldtMDNjTVRhMnpKSWNWOUZrenFpeTlDS0lzNzRyY1U2bHFsSGgvbUVWNWxRUmFxbndKQ2JvYnZGWG4vNDQzL3JjSnduTW1EMnJTOHpOTnZIeVAwdDViR3p0TWg2TjJiOXZUNUZBZzByVmFhTnhSTDgvcE5mdmt5UUp0VEFrRER4cVlSM1A5L0FESHorTDRzcEJuUnViTzh3MEc4ek9OTElOZ09za1UxcmplVDd0ZG9lNWVzQ1B2K2V0SkZHVXlwYXhFSEJLRWNYdzR2ZGU1dEdIamxQejdkbElPaDJKRFBTSEVWczd1Mnh1YnpFWURKbHR6ckJuYllXbHBRVm1XOW5QbWtURlFEZ1d4U3RuTHpFejArVDRrWDFvblVOREF5NWR1OFhwMTk1Z29KcWNmTy9QYy9LSC9pNkQyaXpHQzdLTFVxZUZzaVg3THZZSDRocXdKdk1jMURRa3lVVGdsNUZLMUhmVkNnOE9US1NVOHBTWWVYY0JsdzB4ODd5L2ZGOWdrNEVudnBlMG1uVlNKWXoxOW1jU1grMDZZOHBpWDB5Qk1DdFRyN05ENGIrL3NpbkcraGlOQmRZVU5VWEtheDFQSlJGZUtzUmdLYU9SVktiTHRzcjJxY1llZTlkZlRRV3lmckd1MmxZY1FGdjVDeVhqd2drVHdKZ3BHZ2RueEZLdzVFdEV0RlFmbCtJQTByWlAyUHFZVmNHbzB3cnFNdWJxNlJmNDFHLytlNGEzTGpNVCt1emZzMElRZUtuSEFrQjUzTm5jQnFWWVhWNUV4QkRIcVJ5M1ArZ3pHbzRKZko5YVdLTlc4d25EZE8vdmV4WUp5UTVZRmMzbTlnNityMWxlWEhDb0w2SVVjV0tJbzRUK1lFZzBIaUZ4d3NHOUt6eCs4aGpYYnR4Z1lYR0JwY1U1bXMwR25pWUxDNFdkL29nTEZ5N3oyRVAzNFNtRDFuNjJGczRxQ3FWUjJpTkIwZTBQNlhRSGJHMXNzckcxaFI4RUxDMHRzTHd3ejhKY2swWXR5R1M0SHVjdlg2ZlpxSFBrNEo3TXk1RUFtblovekl1blgyZGp0NDkvNEFIZStRLytPYTNESnhrUnB2RmpZZ3BDY0tFVXpPY2w5Z09QbTV2bk9MK1ZLZ25EdHRESTJnK3FyT3B3MHEvRk5TclpWR1FwTGpKcmRGZkpHeXd3WW82dFhrcktkTVc4SlBselZwaUN0SE01Rm10bHlya0ZFODVVTjkxYmZlVFZqZFNMWk52VmN2K01ya3pYcSt1TGZFMG1SZUtZbFJNbzFtcklVTTM4c0w5bThVY2I1ZkRXcEpDRlp1NDY1WUlrM0FRWEs5WmNWS1lJU3h6eVRsNnVPNjV6S1FOQzdlaXRhZHFFL0Zzc3YzOGIrNXdaUWpMM25pWmhKdkNSOWpaZitld24rUEtuZm84R0NYdFdGbG1jbjgwMkFubjhOdHpaM01FSXpMYWE5UHNEZXYwK1lvUjZMYVRScUZNUGE5UkRqYTg5dE9lbFdtOHBVVSsyR1FVOE5yZDNRV0I1YVo3Y2Z4SkZFZjNoZ01Gb1RCd25OTU1hOVhyNmY4bG94RUtyd1R2ZitqQ1hybHhocDlPbjIrMlJKREZ6OC9Pc0xNMnp0RERIVExQQnhuYWJRYS9Ic1lOcmFUdm1XZkZyeXZZMDZPSzEyOXJ0Y2V2dU5qT3RHVFkyN3JEYjNxVmVxN04zYllXOXF5dTA1bHJjdnJORkVIaXNyeTFoa2hpUk5HOGdNWnB6RjY1eDlzSlZobDZUeDM3eW4vTEFPMytNdnRja2NTd3NxUkZNWjc5Ymx6eHBMV2V0QUkyQzFXaFJoS2JxVW15b1VONkxaNllucjhEQm1VSXhtRHNBbFhhbDZnV3pVRnNCTU5Yb2NTcVNSZnV3cU5pUERlS0FQN1ZGbmk0Q1I3TUlkNlZzSFV6YUhxaVB2SGEzWEY1WmJpWW5GRlFzNEpKOTNGSGhFYUlLSFl6Q1RTY3RoaFdaOGNaT0ZjLzdsUkliTlprTWc0Z1RqdWxJOUt1eUgwVVIrMTM5aFlvemdGUVYyb3lVU1N5cTFCU0FxcWl4eEVsYkxkUmloZGszb1NFeGQ4Kyt6S2YrODYremVmNFZGbHZwdzE3emRmRnpKUUp4bkhCM1k0dHV2MDhZK3ZoZVFMM1dZRFFhc3J3NFR4aG9mQzlJTFo2Wmo4Qll2MUVSTjFrUEZOczdYUWJqaU9XRkJmcURQb1BCZ0NpS0NHcytqWHFkZWhoU0N3SkMzeXRDUlByZEhyUE5Hai82N3JjdzZIZm9EOFlNaGlQNi9SRzk0WkIydDA5L01BQ0U1ZVVWT3UwMng0OGRZdC9xSXA2WHgwKzVmSk9pUERMcEozVGp6aFpoV0dkMWFaWTRpV24zQjdUYlhiWjMyb3pHUTVhVzloREhFZmNkUGNEOFRDTzkwVXlDcVBTQTIya1ArTzZyNTlob0QxazQrVmJlL3JPL1RMQjJpQkcrVTZjcm15T1ovL202dWo0dG43UHl6bEd1b0tmaTF0ZVcra1Vzb2xsWmJFZzVUSlNxOVoyQ2tWZ01yY1ZGZGFwN1VHdXFrbmFWSWNJbTBxZXFUbHlwcmdrbm9TVHFvMmMyeEM1NXloVWNsWUFISmozOHR1QlE3aUUycUhMMUtnREkwcGlnWENhQTNiVXBPMlRCVm5DNWtrd1grRmtsK3pxenhGS2JMYVhBeHlFZUtlc2xJMk9zWXg5QTdqQlVaYjIrVm9JLzZ2Q2R2L2dUdnZCN0g4SWZqMWhmVzJaaGJnWTBtRVFZalNNNnZSNjkvaEJmYThMQXAxNFBDV3NobnZMWjN0NWxhV21Cd0xmU1g1VnR1TEpzb0ZuUmx3aU1vcGh1cjgvMjFnNmU3Nk1VMUlLUVdpMG81Z1dCbjBKRjhvTTIvNXFEL3BCbTNlZkgzdlVVeWFpYkVRRFNFejQyWk45em45MU9qK0ZnUkJUSGpFWWpmTjlqejlvS3E2dkxMTXcweTJHbUNFSlNTcU9Od1lqaTR0VmJySzB0TXo5VHozUUxDclJQbkNScHU3Qzl5OWIyTm9KaVpXbVI1YVZaNW1abnFQbXBURGdTeGVzWHJ2TDYrYXZFeldYZTh0Ty96UEduMzBOYmhTVG9kRHNnQ2FMZFNYa1ZlSXUxL3JQMXJxN2ozdzN0cWE2a0JTYkFvL1lYVlpaa25vS0daWWZ1bEsyejJHU3RLcHVnMGpLN003Rkp1WHlacHF5c2QwK2NqSTdpTGZubzJRMW42NUZUWi9PVHpGUXlRY3FmMWRZRVNKbGlVNWdqck52WWFyZ3FLd2ZYME9QSUtDdjl2M05pVjBJd3ExQVJwYmhuUFdXcDFPd3BwcElKY0tzekFOVlNrS1ltVENWZ1VNWlFWMEw3Mm5rKzh6c2Y1UHBMMzg3MjQzc0JRN2Mzb0Q4WUVrZGpmTStqWGc5cE5CcnB0TjVMZjlkUmJMaTd1Y1B5OGdLaDV4V2owaUowUXFsaW5TUm80c1NrbHQxZWovNXdpRkthZXExR3pmY0p3d0RQMC9pZVJtdGRyT1hFV29lbEdvWDA2M1o3ZlpxMWdCOS8xNU5JMUM5bVFPTE0xVlZLL0VVeEhvM285VWQwc3FIa1lOZ25pUkphclZsV00zNUFzMTVMZS9ra3laNE5RMkxnd3BVYkhEdHlrR2JOSitmVVNUYlNTNU5zRmYzQm1LMnRiVzdkdlV1MzIyTzJOY08rUFd1c0xpL1FhclhZMkd6enR5K2NwbzltOWRRUDhQVGYveVdTK1Qwa1h1am1FNmpKRjBwVnNWeXFtb25yOHZua25nTFVNbDRjNWRLTXhFcnRMVy84eW8xZUJRc3I1VktPVlVVM0tKVWdWbFhSaEZUWDljYjFQTGpoSnFrb1RYMzB6RjFSVGxTOTVWbXUvbUtjdjFjNEFGSGxadG9yYTFJb1Vqa0ltSUpmbGlrbUFGeWJsbkxZZWE3WVlvcGZjaUxUVkV0dTBzWFZqOXZUV2xVWkVEa2wvcVNNMHdzRGt2R1FaakxreGIvK016NzdPLzhKQmgxQ3JaaWZiVEVhRGpGSlRMMVdvOWxJKyswd0NBZzlUUXFOVFh1eFVaeHcrKzRteXl2TGhMNHVlUUtpc2dNdDdSZWpPR0k0R05MdEQwbU1vWmFYODZGUEdIaDR4UXR2MjFNenFJdDFPS2ZobVhsYnArbjIrdFFDajcvM3pOc2dIcVNDbFNRdWg5REt3bEY1WGpHNEZGR014bU82Z3dHZDdvQnV0ODlnTUtRL0dJSlNyQ3d0c2Jnd3o4SjhpM3JvRld2YVMxZXVjZjk5UndpMFpMTWFsVWE1cTNLZ2FVd2FYekdNaFoyZExoc2JXK3pzN2xCdjFGbGJYYU01TTh2bEsxZloydTBTTlpkNDI4Ly9jL1krOURaNnVnYUJqNGxqRzhvMVNRd1h4L0diM2dlNmpKMXdzSEJXTmFFcUlUZGl6UWJTL3A5eUM4Yms4elFSWFdRNURHMDZ0N0Z2ZDFXRzRrb1ZUeWYzd0hHcHlzQmVUVmIyNnIrL3RpRk9rcDhSeEZOV2dvdWFQTGFzbkR1VkFTVnk1NVBPODhlemNrdmp4QzRXOWtsVEFCT2xtQW5ZY3NmcUFaQlBTajNTZE9QTUNXSlZJL253elo2SHVxc1VMNmZ3NkJKUGJVZFpGZVdhTmVkUUZxQlRvVWdzWmJoV0dsOGJoamV2OElXUC9qWm52L2xYK0pJUWVoNjFNQ0FNQTVKb3pQTFNBcjZ2Qy94MnJqbklZUTF4Wk5qWTJtSmxaUVd0bFlYblRtY0UzY0dBWG45QU5JcW9CUUdOWm9Na2pwaWJteVVNL016OU4vMWhrT29UWWJKSnNlZWhsU0lSWVRTSzJObnRZSktJdC8rZFI5aTNOTWRNbzQ0bUJwTkpmUE9lT3Z1ZWJhQnB1b2YzTWdTVllUQklMY3JuTDEyblZxdGpqR0UwR2hLRVBpdkxpNnlzTE5CcXRMaDErelpIRCs5SHFhUkkzVTFkcDZhVXJSWmN4RlJaR0NYQ3kyZk9zYkt5Um50M2wxNi96M0E0SkU2Z2o4ZmVKNTdocVovNGVlTDVOYUtDRWl4cGxvSnlMNHhVczZZcU5XSlZGcTZzdzcrYzJOdVpuc1hMbTQyTTBxRnJpVnd2bm50VU5qT1FvcldkWEZKYWM3QThQOElpQlJXTUJBY1Bwb3BOaUM2Mlk2cDBKRlpkdWRtTzFFT2gvc2VaVGJITGJnZlBaZDJLZG1sU1piY1cvTDVLM3kxMktTUFZUQ0NjdmI1TjV6WWlUdHlSbTcxZyt3Q21MQlR6ekFDdEt0c0dVK0NuY3FGUVFhRmhraGltckpsQkFma3NWa1lHSlRGMWlYajFtMS9sazcvNTc2Rzd5VkpyaHVXRmVScjFFQkIyZDlzc0xNeGwzNG80ZllQSzdOS0pFYmEyMjZ5dHJxQVFSbEhFWUJUUjd2VVpEb1lvb0Y0UHFkZlM0VjBRZUhRNlhlYm5XNFJCa0gzL3BYT2pYUG00YTAybE5FYlN3MlU0aXVuMEJyUTdYVHJkYnBxTHFCV2g3eE40aXZGb3dGeXJ4Zkg3RDNOby94cjdsdWRwaEY2MnpuWFo5a1V3YkpIYW0yMlVSTkh0eDd6NDhtdmNkelNGaC9iNmZRYWpFYU54UkJURmFPVXhNMVBuNk5GRHpNNDA4TFNDb3Vvb1IxZTZ3RHlCMXBwUllyaHk5UVluVHR5UEdLRS9HSEIzYzVzclYyK3kweC9ENG42ZS9ybC94dEtKUnhub1doWVBya28rcFZoV1hWVnBBVlJGdlNwU2NSUFlsdVRLSlo1eEFDMW9UMlpGbVl4NmM5b0dDNTVUOGpiRTRUNG9TLzFYek90MGpzQjNoM3U1aExsa0dGZ3hacFZuWFAzKzJTM0pnd2lxVmo1dHlpSmJYUCtoRTVRd0pjakdPVXpzY05CY2xhU3JKWkl0WDlUbGgwVXhwSE5kemJiTnF4QnNXRlZLMFlOVnJMdlZseno5eFJnTHNXQUhjR3JuKzhnbnFxRkswSjFOdnZDeDMrV2JuL3NER3I1aWZjOHFpM010UkF4UkZOUHBkcG1mbjh0SzU2b0pLUlg5UkZITTV0WTJDL01MREVmcGpDQkpESUh2cDIxQzlzSUh2a2JyRkFTeXZiUEx6TXdNdFZBWElFNVU1VmkxN01CQ3V2TWZERWJzZG51MDJ6M2lKQ0hKU2t4N2R1Tjdtbm90U0g5S0l4aUpDVDJQUnVpeGY5OGF4dzd2WjMxMW1mbFdBMThiNG1TRXhIRksydFZaMGsyV0o1Q3ZabnVEbU8rOThqcFBQWDZLWmowa1NoTDZveEc5M3BCdXQwZDNNR0EwamhCSmFEYWFyS3dzc3pEZllxWmV3MU1LSkM1RFoxVXFtZko4bjNFaVhMcHluZnVPSHNMUHFFQ2pPT0hDNVJ1OGNlRVNTZERpOEEvK1BSNzU0WjlrM0Z3a3hpdnRiSGFacVZ5Umw2aUtmZFlhZnFzczh5SkhjK2M1aVhZUDdiUU1VbWw1N1Q3WHVuVnNEbW1oVHJGMUNNV01xb3lNS3pRNitmdFlLWjFMcGJSVWNnaHRnNktnUHY3YWxoUlNTbVducUpDbW0xZ3JEOGVqTDVQck43R09VS1VtdzBYeXFXcXhKcFR5YTltM3RXaTdRc2hzTHVLdS9aemhUYUdPY3RlUzB5VGQ1WWNzRTRRZW9mS2g1Yk9VN0wvM2xWQXpZNjU4OTIvNDlILzVUYll1bjJXeDFXVC8zalZDUHgyVWpVY3h3OUdRdWJtWklpUGJuQlo2QUFBZ0FFbEVRVlRRd2pVaW1VVjJNQml4czl0R0tVVWpyRkdyZWRSckFVRVFvTFZLQ2J4T1NRbzd1eDFtNmsyQ3dIUFlxQ29EUVlwV0tEekdVVXgvT0tUYkhiTFQ3akNLazVMdWszMGZCa09jSlBoaHdLT1BQOEhCQS9zSmZNM0poeC9sNjg5K2cyOTgrYS9SR0JwaFNCaUcrRG9kSnlWeFJDUDAyYmQzbFNNSDk3RnZkWkdGVmdNa0lZcEdTSkprRzRxMExkUGFvemRNZU9YVk16ejErQ1BVUXk4eko2V2cyZEU0WVRCTXY4OTJ0ODlnT0dJNEh1RnBqOVdsUlZhV0ZwaWZiUkdHQVVwTVNqWFdDcTFEeGthNGZPVXFodzd1d3ljcHZCenQzb2pUcjU1anV6UEEyM3MvYi8rNWY4YkNzVk1NZFMxNzVyUkRaNWlvU3AyWHRuSUFWRjVvZVJOQWpaSnFOSDFsRUNrVkJ2WEVpVkdhemFxNUNkVm51ZG96aTcwdnNxdHlxODFJRDRBelcyTFRZU1oweUphcFlrS2VsKzg5TTJIREpJTHBIbGlPS1dHcUpUZTlaTFlYRGovbDZoR3FNZVAyaVo2RFNNU09TMUpxS3VRRUs0dE5DckNDRXdDTmFFWG8rVWc4UnRxYmZQMXpmOENYUHZHNytQR0lnL3ZXV0ZxWUt5VE1nK0VJa3lUTXpqWXRvNU5pSE1mMGhpT0cvUkdEYkRoV0QyczB3b0JhUFNEd1BJSkFweERJUEFiTVFvdUpDRHM3YlpvekxlcGhxcnhMQ3Q5Uldna014eEh0WG5yREQwWmpFcE9XNHNaSWdmZENHZndnNU5oOXgzbjBpWWZadjc3TzR1SUNZVmlqMiszUzc3WDVsVi83VldJamJOL2Q0Tnk1UzV3Ky9USmYrb3MvNTlJYmI2UUVJay9qS1pYbUV5andOY3pQTmpsKzdCRHJlNVpZV3BpbEh2b1FqeEdURkNpeG5kNklzMmRmNTZuSEhzYlhVdnl6L0hsSVRFSnNvRDhZMGVzTjZQVDdEQVlqQm9NUmNaTFFhTlpaVzFsbFpYR1crYmxaZk45SEs0aVNoQ3ZYcm5Gdzd4NlF1S2dNQlkvTDEyN3o2dXVYR09vYUozNzRaM24wdlQvRnVMbUk2SkE0R2s5Y0R0TlcyWVVRemZIWFdBR3UrYUMyMGc2b2FWaTc2bTN1Z2lzbWI2bXFkMFVabU5qR1c5WHlOUEdhVEptcjJUcUFqNS9aTEkxMnVwSkVWZVVINUxKSzU2QlFxTW5KVThVME0xVmc1ZDY0U2psa0Zpb2ZobnVZVkVoRjFtTEEwZzFOcmw2bTJRQnk3YmlxaERXSUZDU2doaVRjZnYxbFB2bWZmNTI3WjA4ejJ3dzV1TDZIMENzcmsxNXZnT2RwbWpNTjRqaG1PSTdwOVFmMGV3T01HSUl3cEJhRTFFT2ZJUER4ZlM4dGNiVmpaaTJ3MWZuQXhraUs5R28wNmpRYnRhemJWd3lqbU41Z1JMdmRvZHNiT2xyNEFqU2EvZHlyZTlkNStKRlRIRDUybUgxNzExbWNuOGVZQ0dPUzlLQXdoazY3VGEvYjV0Zit6YTltOHdtZFNiazlvckhoMW8wYm5EbHpsdWUrL1J6ZmZ2YnIzTGwramREUHhFUmFvVlJDSEJ1VUdQYXRMWFBpdm9NY1dGdG1jYjZGNTZmbXFNMnRYUzVmdWNxcGt5Y0l0TUhFY1VHMEtkVjhHU0ZYSzZMWU1CcW1CMXUzTjJBMGloaU9ScG10ZVlIVmxVVVc1K2Z4ZzRCYnQyK3pmMjBGUlpMK0xuVXFVMi8zUnJ6MDhobTJ1Mk9DZlNkNDF6LytsMmxHZ2VqUytXbDNsZUp1aUJTdWRtQ0NJYVZ3bnR2L3Jjd3FhN1BseUhTbFlubWZvcG90NHV1S3FuNXlnSzd1dFZHcmlJWUExTWRlMnhUM0JwN2lrN25IYWFVclFBNkhwR1BYK1pXeHZsUlVlNm95clZZVng5MkVhazlWZE84S3kwd3hLVkpDcG9JSHJSQlJ5d1l0cFJOUm16SGVzTWR6Zi9aNS91ZHYvUVkxaWRpL1o1bWxiTENYbW1rOHVyMCtSaUFSb2R2ckVJM0dLYXV2VnFNV2hJU0JqL1lWZ2VkaEJ6czYyZ1pWemg5VXpxTVR4WGE3UTczUlJHdm9qVVowT2dOMjIxMkdVVnpHaVdlT3ZyUmZTWmhiV3VTQmt5YzVkZW9oOXU3YngrTGlFdHJMWFl0U3RFcXBPQ2ZGaEhWM2R0bHQ3L0N2LzgyL3h2T0Q4dUJWWmN5MXlXNktmcWZMMWVzM2VQWGwxL2lydi94cm52L1dOeGtQVTFHVDF1bkd3TlBwbXJGUjl6bDhlRDlIRDY2enRyckVhQmh6NitZMVR0eC9FTS9FMmF6RmxMbUVPc09Ra1pxWmRNNFFFSWppZEpEWTZmWHBkTHAwQjBOR3d4R2U3MU5yTkppYmJYRm8zeXIxZWxBd0cvS2I5K0xWMjd4eDRTb0QzZUNodi9zTFBQak9IMlZZbTB1M0Y1bFUxdTBCWk5Lck1tMFlyaXpyamxLb3lzVlVYbHltOE1JNEYzMDEzY3JhbUZWaHBjNkFSNWdTVnBqVGxLV1luVW1sSjVIcWhmdjdyNlpyUUdYbmkxdjd4b0szNzRSNzVMeDhDNFBzQUJBQlV5bkRMU3VHUGFtODExbFoxV0ZQREZpcThnR0gzMWUxRE9TQkNXbEpuUHJ6OHdaZnlqNUtjZ0dVb2VVck90Y3Y4T25mL2lDdmYvc3J6RFZxN0Z0ZFNVR1lwT3U1MFRpaVB4Z3dHQXp4UFUwWWhxa1AzOU40Z1kvdmVhNThVeWJqckoxRlVGYSthQzhsNFd4c2JaTWt3bUEwcGp2b2tVZ0ZNcVlVU1pJZ0NvNC9kSXFUSngvazZPRURySzJ0RVlTaGt4UWt4cFF1em9MdmwzNS9nMzZmRzFldk14d08rYjkrNVoreHZMSktFSVNaRDExSy80UXFieENUYlRLU09HRnJZNHR6NXk3dy9Bc3Y4ZHkzbitPN3ozMkxtV2FEVnJPTzcvdDR1YnZTSk16UHRXaldBL2FzTFBEZy9ZZXArNkNTaUNST3NreUg3SGVoczF5RXpFOVFzQ2N5SkZxY0pQU0g2VEN4MXh2UTdxWHpnL0Y0VEsxV1kzVjFqZFdWQlZyTkdvR3Y4THlBWGovbXhaZGZaWE4zUU9Qb1k3ejlaMytaNXZwUmh1SzVzeXpMU0ZNOFA5WXFTbFZVcTdaQWJYTDltZ2ZOV1BtWWsrYmJDcWhWVmVUNEZkdThLek1zL1NpVlNIWGxNRFJVS1pmSmx4SWlxTjkvYlVPVVRVbXA2cmluS0hPS1hXUytqdGZhUVhlbko2cXUzdldXc1VkTmNQNG54aGhWUEplTjhiSjBBOVVLb3ZqYWVUa3NsTHQrNjliUGFhejV5dEZrUzF4dEV1cFJqK2YvOGsvNXduLy9IZWp2c0w2MnlrSnJoc0ZnU0xmZnA5dnZvVVVSaEFIMURMd1JoajYrVW1qUFQwSHB4YnJHbmpIWTRaRzJHVUpqREl6R01lMUJqMjZ2ejNBUVpRYW8wcWVSaUpBa0NjWVk5aDA0eUdPUFA4YUo0L2V4dnI2WFdyMkpLRVVVanpFbUpmUGtWSjVVVTIreVlCSXdVY3h1dTgydE8zZTVmUGtpdDY1Zng4VHA5Z0VGQnc0ZjV2RW5uK1R4eHgvbndNSDlMQ3pNcFhwL3BWRjR4YlE2Zi9CMVJ0Vk4yNFdJdTNmdmN1YTFzN3p3dDgvejlhOThtZHZYcjFJUFEwTGZUMWQ5Q0hFY0V5Y1I2M3RXT0haZ25iWGxlWmJtWjJpRVBra2NnVXBMZVUvcjhsN1VGaE9CTE5SRDBoWmtIQ1gwQnlNNm5SNjczUjdkZmxvZENNTGk0Z0w3OSsxbGVXbUpzRjduek5uem5EbDNpVkY5amlmZi80c2NmL285REZTZFJIbVdoNkJTdGNwa1VUd2RkT3VtRGxXekw2VHFjM0c4TGFxc1JpWm92L25GWnlsckhGMmN1SEppcFNhTWU3Ykd3SmtCM0t0cm1aaU1DbFhBL2ZmRER2NGYvU1dWMk9mcDFZQnlKSk1scVU5TllNRmx5dmNvbHB4WmxDTHdOS0hFZEs1ZjVETy8vWnVjZSs3cmhMNm1XYStsdS9IaE1IdFJFMlpiTFZyTmVxcTY4OHFOcEo2eU1jbmJFNk5zbWFZR3ZCU24zVTl2cjNhM1MyelNBRkRKTXQ2U3hLU0NLV05vekRSNS9LbW5lUERCNCt6ZnY4N2MzQUtlOXRKYlhsVXc2OW5FWDJ1TjlqeVVLRHJkTmpkdTN1RDh1UXRjdm5DSjBXaEl2ZG1rVWE4VEJCNUt3Tk1LMy9jeENQRTRvdGZya1lodzlMNzdlUHJwdC9IUXFWTWNPbnFVbWRZc1NpV1l6Q0tjbDN1cXlQY3JQNTkrZjhEVjY5ZDU3ZFd6Zk9NclgrSFpyM3dGb2lnOXBBS1BJSitGaUJBRWlrUHJlemh4N0RCN1Z4ZVptNjNqQVhFMFRuRnFZaktmQVloS3NwYkdMeHBteVhvYkk0bzRGbnE5QWIzQmdIWnZ5UGIyRHA2bmFjek1zTHkwUXE4LzVNcTE2NHp4V1h6dzcvRE9uL3VuNk9XRGpGUlFrSnZTUGJ0VUFUMVQrYUpPdWU1c210VFVkMFNZNXEycEFEc3JHOFBxZjYrcWJjazlNUHNsWGw4cWFkNlNIZ0J2QWsyZGNocFUzUkZUdm5QNVAzanJwN29Bc2FvQU5VRndVWk96d3UvN0E0Z2pNc25WaUlZWk0rTHN0Ny9LLy9qLy9oM1MzU0h3TmExR25VWXRIZHo1WHNEbTFoYXQyU2J6c3pNRldjZkdvNWY2N1d6RmtqdWNDeGFjWmpBY1pheS9YcnIvcG95bXl1c0ZZd3kxV3AwVEQ1emdzU2Zmd3BHalI1aWZiK0Y1SGtsaXNqMTllcVBuR2hPdDNBVGM0V0RFeHVZRzE2L2Y0T0w1QzNUYXV3UkJtTjdBK1lwUjBwczRTV0k4cFFqREFOOUwrZi9hODRvMFloU1l4R0NTR0tWOVRwNTZtRWNmZTRRVER4NW4zL28rYXZWNlZ1SzZDVG5GSmtVRXJWUFIwczdXRHVmUFhlQTdMN3pFbC83aWkxdzYreG9oYWJKd0VBVDRua2ZncFpyTG1VYkFrY01IT1hwZ25aWEZHY0xBeHlRUlNUUkNKQzUvMXphdEozdjdkTWFnVk5vSDVURVlqcmwwNVRvenMvT0VZY0RHM1EzYW5SNmRmbzlCRE9QNkF1LytoVjloL2ZHMzAvTkNEQm90azB4blpSRkg3YkdCMVRXNHR1aHBqMmExNTYva1lTSXU4YkphNms5MUN0cTF0Q3F0eGtXbk1HMzZqY3FHZ0pYd2lxcVV0R3FiTGRzaGNheXlFejlReFZ0aHR3K3UvSGF5QXBoR2ZyVm5MbFZPZ1o0aTVWU08zdDlLbnNrbXp6VmZvMDNDYU9zV1gvejRSL2ptNS82UXVnZkxpM01zenM4UmVPbERsSmlFdTNjM1dWeVlwOUVJTStHUnNTTUVIZmhqamtZVFlCd1ordjBCdTkxdWl1azJhWXF2RVVWaURKN3lNQmlNSk93L2RJU0hIMzJFa3c4K3dQcStOVnF6TXlTSkx0RGsydk13Y1VLU3hHVkpMNEwyRkZFVXNidXp3NDNyTjNuOTlUZTRjZjBtbnRhRXRUcUI1NkU5amFmVEYza3dIR0lRNXVibldOMnp4dXJLQ28yd2hra2lIbnZzRkhjMzduRDkyazB1bnIvTTV0WVdnUmV3TUQrTEtNRWtnbEZDTkJ6VDYvZVltV254MW5lOGc4ZWVlSno3N3ovRzR0SVNZUmhtSk4zc2M5YTY5R0pKbWJJVVJTTnVYYi9CK1hNWCtkYTNudVBacnozTGpRdm5hTlFiQko2SDcyczhuVStmRXRiV1ZyanY2Q0hXMTVaWW5tL2lhNDFKeHBDa3JrUHRaVkoyVEpabmtCOFFPYXpFNStMbDZ3UzFrTU9IOXBORUVZUHhtRXZYYm5MMjNDWDZTY0NSSC9neDN2NVQveENaMjBPRVgrVDYyYWxGN3JDdTBucWlKc3BWcGU1UjJWWmZTR3NJYjJjTHVnNUVLeUJFcktTZmFYNllpZFhDcEExQmZlek1wdGk4TUtyck0rVXlwRkp0dkM0SGZsTGR5eXRyYUNJMkc5VWFYdGlRWlNvR1ROZVlwTlFFOU5qYW9kcWtXQ2x2SVVvMGN4WEdiTEs0TlE5RDNZeTUrT0p6ZlB5REgyQnc2d3BMY3kzMnJTM2o2VEwwZEJ3bDNON1lZRzE1aWREWEU2MUpPYzNYQ0pva1NlaVB4dlQ2ZmJyOVBsR2NGSHB1MnlzK2ptTVdWMVk1OGVBSkhqanhBUHYzNzJOMnRvWHZlOW5ob3JOVExhVlBpRTdsckNvTHRoeEhNZDFPbDl1MzduRHA4a1V1WGJ3SVJxaUZOWHpQejRMSERKSVlpR05rUEViNmZXS1RjT0NSVXh5NS94amE4N041aXNJa0VVUWovczVibnlTT1IybXdxdklaRGtmOCtmLzhQUDJiTi9GYnMraVpHVlRnazJRSGtwY2w4S2IyNENGNzl1M2owU2VlNEtHSFQzSGt5R0htRitiUnZ1K2czUEtCYXo2L1NRTkxZTkFiY3ZQbUhWNTk1UXpmZVBaWlhuanVtM1MzTnFrMzZ2aGV5amYwZkJpUFIvZ2UzSGZrRU1jT3JiTi9iWmxXUFkwaFM2SW84Mm1RelN3S0xXdXFwdFErVjYvZlpoeEYzSGZrSUJBalFHOFE4OUxMNTdpMXNZMWVPY1M3L3RHL1pQV0J4K2dUSUhobHp5OFZzN3F5c1ptVDkzRUJ4YkhTakt1UysycDdtLzhqclpXTHJSZmIwQ01GTXQwb1YvR25LcVYvRlhTakxOR2UrdVNacld3NVZMSHhXbEZFVW9ScllVM0t4ZXBEM0lXN3FRei9SSEpUaERoU1ltUDFJMHJsUmhYMzdQTHNMRFZGTWJCVFJqdE52a1VHY3ppQXhraGg3OHAvUGw5aWFHL3dWNS81QkYvNnhPL1IwTUtoOVQzcExTZW1hRFlHb3pGYjI3dXNyaXltNkMxcnl5RlpDazJjQ01OUlJMcy9vTlBwMHVzUE1LUzlkRHJSVm9WSnhnOXFuRHgxaWxPblRuTDR5QkdXVjVieGZaM0piazA1ekJHclZaRnN5S3FFWHEvUHhwME5MbDY4eUlXTEYrbnNkcW1IUVNxSzBXbVY0R1hHbldRMFp0QnBFN2ZiZUlNQk0wWm9LaUdlYWJMK1F6L0UvbU5IU1pMVWNXZkVrSXhIRUk5NTY5dWV6UGJ5UWxnTFVXaisrbE9mb1hibU5aSW9aaXRPMkZHS3FEV0x2N0JBYzNZT3YxSEhvSWlTbUNReGFWc1JSeVJKelAzSFQvREk0NDl6OHVSSkRodzZ5TnhjS3cxcXllM05hZjZRbzRmTncxcDMyMjB1WDd6STkxNThpVzg4K3cyKzhaVy93dk04QWkvOTNmclp5bEdTaUpXVmVVNGNQY3lSZyt2c1dWbWtVZk13U1V3OEdoZVdNS1VVbnFjUnBibXpzY1B0MjNjNGNmd0lnV2RReWdNVmNPbjZIYjczeWhzTVZNZ0Q3L2xKbnZqUm4ySFVTS1hFcnQ3R2tndmJnaGNtVldyVmx5OEhEWmErZlNuazl2YWxxakl2VFA3T2xSa2E1YjlySmxnRVU1STJaWXJJS0Y4NWYrS3NyUVBJd0FSU1lhR29rcmxuTXR6UnZhb05tY0lwc1NlbkU0bEZOdFNyTXZHVEtmKytVeDFZRllPV0V1UmovMlV5c28vbit5QUdQeGx6Kzh4TGZQdy9mWUE3cjcvQzBteURRK3Q3Q1R5ZDYrdEFLZnI5RVlQQmdLV2x4UUl4bGQ3K210RTRvcGZaWDl1OUhuRldWcVFEdS9SbGorTVlBeHc3Zm9Jbm4zaWNJMGNQczc2K1RyUFJTSTlVWXdxYWNHNmdTUUdpYWQrdFBZL2hZTURkMjNlNWZQa3FGeTZlVDFuL25rOVFxK0g1WHI0Z1ErS1k0WEJBMU85anVoM3F3d2dWalFrbG9Za1FKSUkycWFXM045dGd6M3Zldzc3N2ppTnhPb05JakdFODdFTVM4ZlRUVDZLUzlPZjF2QURQOC9ucXB6NU42L1JMQkZGRUxKQm96Y0FQdVRFY1VXczI2WHNlbytZTWVuWVd2OUVnYk5SUkdTWk1CS0x4aUc2M2h4K0UzSGZpZnA1NnkxdDQ0T1NEN0R0d2dHYXpsVDA0WnNKQktnV29Sb2pHWSs3ZTNlRHk1U3U4OE1LTC9QbWYvVG12di9JS29lY3hPOU1rOUQxODM4UFhRdURCNnZJaTl4MDl5TUgxTlpZV1pnazlUVFRzazVpb2NGcnVkb2FjdTNDUlV5ZnZaNlllWmprR1B0MUJ6QXN2dmNydHpWM0NBdy93cm4vNEw1ZzdjcEp4SmlXMlZacktxWTRyN0lCSzMyMXIvS2VOckNhZVgzRVZmVVpWOHJmVlpPcnh0QlhqNVBkUjBvelVKM0lnU09IelNpYkJ5c1ZPVkdNa3lVNXRLLzRxUnc5THFheFdPWmROc0laaDA5cUxDcXhUN2pGOXpMNitVWlRSejVVVW42bzR3bjZJTkFuMGQvamE1LzZRTC83K2gvR2pJWWYzNzJWbEllUHpaYWhxbEtMZEhZQVk1dWZuUUNCT0VnYWpFZTF1ajI1M3dEaE9DaDVnemlVeXhwQVlZVzUrbmlmZjhoUVBQSGljZmZ2WG1aOWJRQ3NoTVNtUjE4dTkvWGxRU0JiL3BCV014MlB1Ym14eTQvcE56cDA3ejgzck4vQThUUzJzWlI3OHRMTHdVRWcwSXVvUFNOcGRrbTRiUDA2b0dVUGRKSVJLRTJOQURBM1BReVdtT0p4N3MzVlczLzFEN0Q5K25NUVlsRW1yZ05Gd2dJb2pubjc2cWJUbVVxa2ZRV3ZOMS83d2o1ajU3bmNKc2d3QXJZU1I5cm5iSDdJODAwb1BGbEYwTUhTVVpsU3JvZWJtQ1dabjhSb05SR3ZpVEpNZ2tyN01vK0dJeHN3c2IzbnIyM2o0c1VjNGR1d29hM3RUL1FMb3pFc2dKVFFSUTVLajV4VkU0NWc3dCs3dzJxdG4rZmEzbnVQTFgvcEx1anNiaEw1UHN4YmkreGtOTjBrSUE1OURCL1p4OU5CK1ZwZm5hRFZEdElreFNjSTRNcHg1L1R6SHMzanp2STgzNG5IeHluVytjL29zSTMrV2t6LzZzenoyekU4UzFlY1JyUjF5bUs0bVVrM1pvNm1LL3Q4ZFlFOHg4aGZrWGx1ZFdKMHh1QWVIS2R5cjFVdmZEVDFWUllTWm9QN2c3S2JZdnVWcUh5OTVXSmNxZTNaYkdJVElKTm1uMHMrLzZiNVFLckFDcHF1YzdIUlVUZFZRVkJBQjNJTkZRUzMwOE9JUnQ4NThqMDkrNk5lNSt2S0x6TllDRGgvWVN6M1FwZEFwKzFCM08xMzhJQ1R3UGZxREVUdnRkdXJTRTVBczE5QmtBN2pFR09yTkdSNDQrUkNQUEhxS2d3Y1BzTHF5ak8vNUpDWk8rMml0TXBXZXpqQmQyYTVYSzVJa1ludDdsMXMzYjNMK2pRdThjZTRjQXRRenpMZktFbSsxUUpLTWlZWWpvbTRQM2VsUUg0K1lFVVZkSktVVVpNNU5MNXVMRExNcFNNTkwwZGw1M0h5N1ZXZmwzZS9td0xHamhYdlBtSVRSYUlnYWozbjY3VStSTjNHZUg2S0Jaei96V1pyZmZZbDZuSkF0QlJpSng5MWVuNVZta3lDakpJak9uSnllenhERnJvRk5nWDZ0aHN6T1VadVpvVGJUUkFkK1prSFdtTVF3SEE0WWp5TVdsNVo1Njl0L2dGT1BQc2JoSTRlWVgxaEErVGFpVHF4aFhEWmlsSlRoMk9zTnVIVHBJdDk5Nlh0ODljdGY1YVcvL1J2R3d5NWhFS0NWUnkzd3MyYlNVSy83SEQ5NmlDTUg5N0p2WllGNnZjNjU4MWRabUcreVoza2Vpa3NPZG5zai92YWwxOWpxRGduM24rU1pmL0l2V1RyNkVMR3FNWWhIWlRzdXRyTFVibGtyM2dLcCtIN3kvYjJkYkd3YjJpb3kvMm80NnBRZE9sVlRUOG5tVkU1VkFhQSs4MGJhQW1nSGxsMjZBTWxGSHBZQ1VXUlNJSlRmdEZXUmtCVzRuanJXTWpTVXJWWVNTN1NUQ3oya09MR2s2cDdNNEl4bDhHY2VTaWppV29VOWhOcTR4L05mK2hQKzhELy9PbjQwWk85cW1tK25pNEdPa09BUng0YnQzUjM2Z3pGUmxONE8ydE5aYjZZeTkxcUNVaDVIanQzSFE2ZE9jZXkrWTZ5c0xORm8xb25pcUdMdGxISS9iZElCbEZJd0dvL1p1THZCNVV1WE9mL0dlVHE5SHJWNm5jQVBnSFRqb0kyQU1VU2pBWEduQzkwdTRUakNFMEZGRWJNb2ZERjRTQXAxVU5haG5NVnFqOFJnbE5Ed1V1eUpsL1hWN1prWmxuN3dIZXcvZWg4aUNVcnJWQUFraG1RMDVQMC85ZmZZMkxoRnY5ZkhTQ29vK3RiLy9EeU43NzVJSTBtS2ZjdFlQTzUyK3l6UE5BZ3hLQ1ZvblNjc3BhSWxZeFFqcGVoN0FWZTdQUnJOSnBIdndkd2Mvc0ljL213TEZkUkE2NHhNcEVqaWhHZzBZaHhIM1AvQVNVNCtjb29URDV6Z3dJSDl6TXcyczNsSS9teDVGdUlxZzRtSUFqenUzTG5MaGZQbmVlSDVGM2oyYTEvanRSZS9RNzJlSHV4YWF6eFBFWGlLUUFzcnk0c2NQWHdFTXg2eXR0SmkvOTVWTkVrYWFRZUkrRnk0ZG9lemIxeGk0TmQ0N0gyL3hNbDMvQWhSZlpaSXloR3pJKzh1QW52U3p6K1BKQzhKUzFTU3BwbTRnTzNFTGx0cDZNcUh5L2ZMYmtIS2pZTTlrTFJGUU5uWC9OeTVMVkU1VHRqeHdhVzBISlBaV1ZNUlRKbTZtaXZ5OGo4Z0p3YW5pSERMUkdFN0d5eEdlbzdqbTFSU1ZSSlU3STJvVXZjUUtKWEhhdnIvR2hyS3NIMzVEVDc3dS8rVmM5LytLcTI2ejVIOTY5VERrTWdrREljaitvTWg3VjZYVG5kQUhHZHJOWlhtNWVWRHRTaU8yYlAvQUk4OCtnZ1BuampPM3ZXOUxDNHNaSEFkVTZ3c3BWRHBwWkxYZkxYYTd3L1kyTnppMnJYclhMNTRpZHUzN3hEV2FzelVHbmllQjE1R1Ywb1NvdUdJWWFlRGRIdjR2UUZoRXROQUNERDRRSlNrMzJOZGVjVStPajkwY3dtdENCalJEQktEMFVKREszd2xlTm5uMm1tMldIN21HZTU3K0JTdFZvdFdvNDduK1hSMmR0bmQzdVRYL3A5ZlEya1lqa2QwZGp0czM3M0RuL3pXNzFEL20rY0l4a09NOGtpVVpwaG83dmJUQTZDRzRHbGo4UXJUTXhtVEVtMkdDdTUwKyt4Ym1DUDBZS3cwUFF4dE5JT3doc3pPbzJkYmVETk5sQjlnZ05na3hGRkNGRWVNQmtPQ0lPQ0JVdzl4NnVHSGVlaWhrK3padjQ5V2F6NXJDWFRKN2MyM05DWXVCay9EUVQ5dHE4Njh6cmUrOVcyZS9kclh1WGI1VWpvNzhIUmFwVUdhVEtTRS9ldHJIRHQ4Z0wyclN5ek9ObE51b3gvU0cwUjg1NlhUM05udDB6cjJHRC80czc5RTQ4QjlES2s1WEQ0cDZ2YUMrT21XOUU3bDdQb0d5dGExekNpV2lnL21Yc00zdVlmK1JlRktqQXVrdUlENnM0dTdLVHRBMmVSU0tmb1RrK20rVFhZN20zeTlKSGtsa0ljUUtJdEhYbWI5aXVNWG5IQVpNWm5IVW8zUDFxbkN5Mkw1M1VPSWlPY3B4RVQ0Zy8rZnNEZUxzU3pMcnNQVzN1ZmVOOFk4WmtiT1EyWFdYTlZkMVYwTmlrTTNSY3UwS01vaUFYRXdMUmlXYk1pd2JILzR3d1BnTHhJMjRBLzd4d1pzZ05DSFRNbUNRVm1VYVpvMG02UjZLblkzMlhQWDBOVlZsVldWUTJSbVJHUmtaRVM4aUhqRHZXZjc0MHo3M0JkSmYzUlhWV2JFRys2OTU1eTkxMTdEQU4vOTEzK0UzL3Z0L3hsMGZJaUxHK3RZV1ZwdzgvaURRK3dmRG5BeUdxS3lMdHdqTWFVc0pyWkdxOTNCWno3ek9xNDljeFVYTGx6QThzb1N5cUxscXlGSkNTd3FHTlNGZ0RCRzR4SDJuanpCL2J1YitPald4N2kzdVltNkZyUmFMUWZjc2JzSkxXSFllb0pxZUlKNi93RG02QWp0eVJodENJcmF3bGoydG1VQzR5dWFrVGdPUWRzWWlQV1dVMzZUWis4d0ZDYXNKM1VOSzRRMkNWb0FlcVZCdTFYZ3FEK0Q5Yi8xaTNqKzlVK2pxbW92YWlJTTl2WndzUDhFLzlGLzhvL1NESjhacUNiNEo3LzVtN2oyNHgvQW5KeGc3MlNFM1lOamJCOVhlSEE4eEptNUdYUlpYTFpDRUtCSUNrY2xDQ1pzc0hNd3dObjVQbHJSbjBsZ2lWR3h3WmdKUnlBTTJHRFk3V0xjNndQZEhxaFZnZ3JqUnFIK3lLenFDdVBqSVhyemMzaitwVmZ4NmRkZXg0VkxsM0RtN0JtMDJsMzN5bUlCc3RHTVJVS1ZXRHUreGVCZ2dOdTM3K0h0dDk3Rk43L3hsM2p6Uy84YUxCYWRWdWxDVTcybG5ZRmd0dC9HcFhNYnVIVCtMRlpXbHJDMHVJUmJIOS9CRDk5NkY4T3lqOWYrN2IrSEsyLzhMS3IyckJzRCsrcVdhSHJXVHhrZUpqbE5YeWhXdkhLYXgwWm03b0dNWGh4ZHNPSm9sVE1VVVVoRDhwS0xuYjkwOTFDU1pWVVNOZGpZWndHMXAxZlcxaEZnS2dtYlFsQTRXVmdMMUVvWUZMOU1WTzF4bk1lbVlFQmYwSXQxM0lKVEE1Mm5aSU9uaURLOUlvNXExTHNQOE0vK3gvOGU3Nzc1SjJnWEJtZlcxbUNyQ29lSEI2aDlUMjVEcVN3MUpsVUZNZ1Z1M253V3I3NzZNaTVkdll6VjFYVzBTZ09RKzE0aU5lcmdENmNTWE5oakVnZjdoM2o0WUF1M2JuMkVUejc1QkVjbkp5aUxscHRkZXhLS003ZVpZSEl5UkRVWXdCd1AwSzBxbExWRllTMW15Qm13eEljRUtYcys1QldNclVVdDFqc0doOGh4bjFOSGhOTHo4c0dFa1RqdnhQV1pQbnBNNkJRTVpzRmUyY2Jjei84Q2JuNzZWVWR5OGFmTDRkNGU5cDg4eGovOFQvOVJDcGN3RE5RVi91bHYvUlp1dlBjZDlDWmpBSVRLRWg3WkF0OTQ3eGFldVhBR05Eckc0T1FFWTh1b0xXV2ZxeUJDeFl4SGh3T3N6L1pSVXNnNTlBN0hsQjU2SWNLUUMzeHljQXpxZG5FQ1lOUnBnMmJuMFpxYlFhdlRCUVg1TVR0ZmdzbWt3bmc4UW45dUFhOTgralc4OU1vcnVIejFHaFlXRjhER1ZZT2h6WnkydGlaVWxjWCszajQrL09BRGZPLzdQOENmL1BFZjQrNkg3enRhdERFb21ORnRsMjR6cVN2MCtsMWN2bndKcFNsdy85NDlETVZnL3JuUDRndS84US9CeStkOGxaeWI4RUNON3pBMUNYQ2ZqWVd5QUFCTG9xWm9PV2dXMXBPbXYyczh6Zm8yV2FkWlJmK0NrR1hnTjRXaVhTVHhBMG5JU2Zjb29mWGxITnppcjBsZ2E0cHBxNVYxYlVKZFMreHBMS21BalJnV3dTcll3MDBJNEFHMTJnWktaNTJDbm9XVkZscFNTd0VkaDB6cWU3dFBQTnN5K0ozLzdSL2o3VGYvREtYL20rMmRiUkFjdWFhdUxZUnFrQ213dG40R24zdmpEVHh6OHhwVzE5YlI2M1NjYmJVNGp2bWtDdVFSZDAxSzQ4QWtzVFdPam8reDlYQWJkMjdmeG8vZmZ4L0h4eWRvdDlvb3l4WTY3UzQ2blI1UVc5U2pFY2JIeHhnZkg4RWVIcUExR3FNdkZpMnhhSXVnRUhjZGE3RXd4bzMxUE84blZrOWljOWNYOFp1T3k4UnpJaDhYTjE1Z3Z0MUNyeXpRS2x0NFBCcmpZRGhFcjJBWThhMkpBQlZiWll5S1hHamlyWmhDS2NyKzRMVHdaSjI2anFkOGh4a2RVK1BTMGh3V3lqbE1iSTNENHpFZUQwN3dhSCtBNDlIWTZ4RU1XQmkyQmlxUWM1bnlVeU5XNlV6czUrT21tcUF6SHYzakhDNEFBQ0FBU1VSQlZHSzUzVUtMQ0tQUk1ZNkhKemg2QkJ6Q29KN3BvN1c0Z043Q0xGcmRQbHBGQjcxZUd3RHc5bmYvQXQ5Kzg4c1kxeGFMcTJmdzJtYy9nMmVmdllGTGx5OWhabTRXeHBRNVlZZUFvbVd3dkw2STViVTM4Tm1mK0J6Ky9uL3dEL0RnL2ozODZOMTM4ZWRmL1RyKy9HdHZZbnQzQzcxT0c3MU9HOGNUd1RzL3ZnVlk2ejR6TWZhLzh6VzhjL1U1Zk9wdi9ScEdjWUZLRHNJcFBvQlZXSUZUMVZyRmRhRUc4UzJ2cm1OVkhRTkpkVXAxdzg2Y1VucVJLQUtRTXdCMm9xNmlaU2h5MWJrUnhDbGVmVldMSzBPdEJTb0FoVDhGbU1UTndBM0ZoRmF5dWJlZnpmejRKZXE4dzA3R25DUytoZ092UFlsMFV0eVlNci8wVnRuQlRJTDlCUnBYd0V1dmZ3NWYvVmUvNnpUcHhGNGhOOEhLbVRONC92a1g4T0tMejJIajNGa3NMQ3k0SG44eTlxTytDbGJxM0xqVFAvamowUmc3Tzd1NGQrOGhidC81R052YjJ5aUxOdnE5TGxwRkc2MzV0cnVaZFkzSnlUSEdSMGVRd1RGNGNJUmVWYU1sZ3NMV0tQeDFaQlV5T2dGY1VDUlU5Qm1GOW9sZ3ZaMjNEVnVrdURGb3Z5QjBpaEt0d3Z2L0U4T0lnR0hCVXNGSWpaSUloUUxJSkhvMUJJRFZVNWExakJ0Skwyd0RtQnI4L29MazFHTWtKVGtxTlU5cUZIYU1lV2JNekxSd2NXNEZFeEFtVm5BNG11RCtrMk9IS05VMTZrRHdvc1Ruc0VFOWlSUW5WaExRa2hvZEljekNvaEszT2Nyd0dDZGJKeGhzYmVIQWxMQnpzMmpQemFFOTYweFNlYWFQamhXTWpnN3h0VC83RTN6eEQzNGZ0Ylc0OXN4TnZQS3BUK0htczlkeDl0d0dldjArMkJTUndlbndDMGE3VStEU2xjdTRlUGt5ZnY1di9pSU9Edzd3eVNlZjRKMjMzc1pYdi93VnZQMjliNk1lalZBVWpLSXdvSUpRV2NhbG04KzUrME9ubE5teEluS3RzOHR6VEdRdkl2ZmRJRFk3MTZFMkUrWUUrcEgzVUFDVGFpc0VSdE9PVkRvWDB5a0dQZDQ0eFpsVmMvcTRuQ1g0T01UVitJZkhza1VwRHFCaElyQWxNQmdzUUdXdGUwZ2lMZGFWejRFZkgrWEdwQkpiaGYzY1VtS0xUd3JvQ3g1czBYeFJLd0ZEempHekY2TzVzdmZGbi80NS9OZi8wMi9qVC8vVnY4QmJmL25uV082MzhXdS8rbmR4NDhaMXROb2RGelhseFRRZ1FsbTBZUDBKeVRDQU9BdnIzZDA5M045OGdGc2ZmWVM3ZCs2QjJMZytuaG5kVGgrRk1aQkpEVHMraGgyTlVBMEc0S05EdEtzSlpvUmdKeE9VQXBSV012R1VscEphd00rMmZidWxySjVERzhYTzl0VXZRbGVkcmMzT29HY0lNSVFKTTRZQ1NDM29BbWg1RU1xS2dQMm15cFo4eXlaUjR4OW5WMUNSVldGY0ZObWFpUS9pS2doSkVYeGluVXV3V0tDdVlTdGdVQlRZa3hxR0dIT3RBaDBDK3YwdWVqTjlQQmtNY0dGNUhqSVo0V2c0d3JpcTNPSW5odlhPUitRM0hSczJQbmJQRmdOb0dRS0xnS294WmhoWVlzYk84UUREd1Q2NiszME1yTVZoMmNaNFpnWThNNE9pMndPVkJyMXVGOVlLSHR5N2czc2ZmNFIvTVR4RzBXcmpoWmRleHF1ZmZnM1hidHpBbVkwTnROdXQ2RHhNVkVRRWZXNWhBUysvK2dwZWZ2VVYvTXF2L3lwMnRoN2c5Ly9sdjhULzgvdC9nS285Z3pOWGIrQnpQL2NMV0wzMkhJYmkyS2I2OUNaVlVhWERyMEhTYVRwU0I4OEhrVmk2Yyt6bmZhUkxVdm40UXlGSXM1UFpicGppeEtRaW5TYnNYN0Z3K214TzFrekJURkRjelNDdkxSZi9ZdFovQjBNV0pvenZyTzl6SWFpQ0ZaVlFTbGlJWXo2Vk5vS2tGWWhJYVp4WnNqSmNDRHVlaWpXU25Bb1ZLWlFXR0ZLQk15Kzlqci8vL01zWTcyN2hmLzJ0L3dvek16MkFnUEZvNlBYeHpnd2o3TEJWWlhGd3NJOEhtdy94OFVjZjRhTlBQc1o0UElGcE9jWmRmM2JPc1JTdG84eU9UazR3UEQ1QmVYS0NiajNHakRBNkltQmIrUnRCR05yS0JYbHlHUGVvM0RabHBSVE1OYXdQNm9BNDg5R0NDWjJ5UUtjc1FjeW91TUNqNFFoUHhpTWNGZ2FQU1hCU0dIUXZYRUpuZmhINzJ6dVEyN2R4RVl5V1dJZmJaQWFLV2p1UlFMb0kybEtpdGNhd0ZLWERFQyswQ1p1eUcxRzVFWmRsd3FPYThaMmh4ZEg2T3A3NTlHZnd5ZjM3R0gzeUhoWUREdENmUWJmWHhuTFpCbnRQZ05Hb3h0N3hFWFlIUXh5UEo2aklBRnlxc1ZxdXQxZm5GQW9BYlJHVVVtR3BIbUhGQXVONmdvUGpJeHhzYjJOQWhGRzNDNXFmUTltZlFkbHBvMmlYNkhhWFlBemovdTNidUgzclE0eEdZOHd0TGVINWwxN0dDeSs5akV1WHIyQnhjUUZsV1RyY2l1bzBSaXVBdFhNcnVQbmNNL2p3N3V2NHdyLzdIMlBTNldOQ0JjWmtGSEFkdndGc05KYjM4Ym5hWVN4TDFMWnh6QmZjcllRb3ArZUhTa0x0R0NMS2d5aE9HOFJYa2VIWTU5d3lUNUp6VUJGV0RvTjkvKzc3YlBhQWxFbUp1dUVEbG1KUVd3SVpwMFUzbGtDMUEvUEkxcGhJSWczVWNEdDNlRzFTaVN2YUhDR2JkWXAyTHBNc25DU0tHQ2lNWENoalhnSUNVN1JoaXpaYWl4WlV0TUdLSHhYZTkzQi9IdzhmYnVIV3JWdTRjL3NPVG80Y0FhZ29TOGMxYndIR0FESWVvem9ab1RvY1lESTRSR3M4d1J5Uk8yMGhZTEVvQWpjeXpNSFY3Tlp0QUM2QUk5Tm54M1JoOTAwTkdMMlMwV21WNkhnM0lUQmp3b3dUQVB0aThWQktGSmN2WXVhRjUzSGg2aFdzbnoyTG1ZVTVQTGovRUxmZS9SRzJpVkRmL2poV0FGWUZtVEpabGVJd3piNEtudkdKbmUxTVNtS1ZJQWxkZGx3REc2ZENBd3Y4NE1FT3FsYytnOTZaTmZ5YnYvSDNzTEM0aEtPRFBkeS9mUnNmdlBzdVJsLy9KcjUvN3lPY2F4bXNGSVFaV0hSYkZtYzdCZGJtWnpHdUJVZmpDanRIWTJ3OXJzRzJjbVd2TDJyWmVxcXcxeEFrKzJ3TFdBRmJRY3NLRm9rdzYvKzJPcTR4T0Q3Q0FRZzcxcUk5UDQvdTBnTE03Q3pLYmdkRmkyR29SSDEwaUI5ODQwMzh4VmUvaExvV0xLK3RPLzNDOHkvaTR1VUxXRmljQXdybEhNMEU2dlFnL1VWVXB1MDJRVnM3b05BbUFsQ1V3a21JbDlOWkZVb1o2UDBmVXZydnRLdFZQdTdMYUc4S003QnA2Q0NTU2Uwa1pCa0lzaGFsQ0doZzdSSDY2TTJXMllOeEZveEgzdS9OZ1VadWRpb0dJT3ZLLy9Cd2NBd0dWUjUrcDdKOXFhRjFGdld3cHZHR2hjcHRsOVRvYUVFRUVhTzIzaGVObkpDR0RHRjRjb0xOelFlNGMrY09QdmpnUTV3Y242RFZhcVBkS2gzUzIrMWlPSFFrb1A1TUg3MnlRSDF5ak1HdGp6QmJWV2piRktuY1k4REVma2IxZXhLd2tLQk9EUDU3S1poQ1BBaGtBSFRLbHNNaDZockxDM1BPS2NjWTdESGpCSVFqWW1CcEdaZGZmaGt2WHpxSEw2eXRvRGM3Q3pZR3RkK1VKK0p3R090Sko4WlBKNEp0bFQ3ZFhmWEpVeDRNcVRxVjNPUE9RWTB4OFlsazJnZUNtVENxblB5V3lXOE1JSmpDWUg1eEViTUxDM2ptbFJmeDg3L3l5M2p5ZUEvM1BybUwyKysvaisvODVUY2dEemN4RDJDbFpUQWpOV1phTlZxOUx1N3VQTUlMRjljeEdaL2c4R2lJMGZFUUZTeVlDMGYwWVlvaEd0YnIzbU9WSlVEcGtmWTJhdlFFbUdkQ2VUTEVhcWNGMmpyR3dRT0w3YktONHV4WlY0MlJ3Y2xvaExJczBlMzFVUjBQOFBhMy94TGZmdlBMT0o2TXNYNzJQRDcxMmRkdzQ4WU5uRHQzM2oxblhuRloxM1ZTN05rQW9uc2tQdWhuNHFtc2pXeWFvVFJXS1FRcGZwZG10cE1Hd3FrUklVTEtDWWdhaHFNaFNwekp0WVBoSUMwb29vdzVHU0VFQlFTWGxVZ3BKaVFUUjRqenZlTmFPUmF6THk4ZHVoMitsS0ZjN2hRMUF6R0dXd1dzb2VINTV5V25SQlpNNXFtR0l5SGdSRlJDc09VQy8vcy8vMTJjbkJ5ajIycGpacWFQb2l3eHZ6QVBBQmdjSHFEVktuSHg4a1dzclcxZ2RuNFdoL3NIMkg2d2lWbG1mUEwrQjVpREUwQ2RXRUVONjA5Q1NZc25YQ0wybm5iZWdqdU0vd3IvZ2N2Q08rQ1VMV2Z1UVl5cXFuRlExOWhrZzJNbTBQSVNMai8vUEs1ZnU0NnpHeHVZbVY4QU1UQ3hFK2ZyQjZEeWk5K0tCV3BCYmF0NC81Z1pSU2pUL1Nab1BYQVlnRnFKNTBVZDA1Y0NBQmZPRmlIMlkwNktVVmd4NjRGYy9rRG9CZGlEZGhNRUxLaUNTQldmRmNNTU1pMnNuTm5BNnBrTCtOUWJuMFAxNjcrT1J3KzM4TW1IUDhhdHQ5N0IrMisvQmZQb0FXWUVPT3AyWVdhNldDNTZPRThFcVdzTWgyTWNuZ3h4Y0RqQTBja0lGUmxZRWYrZUFxcHRJZ1JSN3JMY0FXR1dMT2J0QkcwU0xETkRSa09jZitGNVBOemJ3MDkrNFdjQU50aloyc0hkTzNmeDhVY2ZvVjJOTUR2VFI5ZTJNVHJjdzVmLzhJL3dmLzBmdnd1WUVqT3pjK2ljdmVFaTZqeUpLRXcyQ0NvT1Q1WGRRYWdlZ2RYc0hKWTR1Q09rUUJyS3dqL1RoaHYvekx1MWtrdzdhUXNhNmNSRTJ0VThabVFXRG9YVnhGMmRYbUtUbXdqeThFOUI3VTBieVNXMlNJMkN2WXBNM0s0WFNBZ3VrZ3B4N0VFZU9JeWJBWnRFRGM1VWc1U3htVUNjTVo2U3J0bVZzT3czc2pnN0xicjQ5Lzd6L3dhMzMvc0Jmdml0YitLamQ5N0M5dTE3NkhmYjZMVGI2SGJiSUFqZStOd2I2SFE2RUM1YzZSemNjcTBEekl6L3hzemllZGpKK29wOXVSK3FFMWhQaVFXaFl4d1kxaTZjWkxlQ1lFU01ZeUhzVlJYMnBRS3RyV0xqeGcxY3ZYNGRhMmZYTUxlNGlMSW8zUlpMZ0pVSmJCWFlmazZTeXNJUVc2R0N4eVdxR3JhV3FDNjBjUVBWWWkwVjJxTEdWR2tXbmVLblJISndLaEs4L0Q5dEVIb0JMaUNVYW1XeEhWaEF0YTgrR0JCMmNsdXd4NWtzVE1GWU83K085WE9yZU9Pbi9ockd3ekYydG5idzhZY2Znbi93UTN6L3cvZlFlYktEVlVOWUxrcjBld1dXZXgyc0w4Nmhua3h3T0p4Zzh2QXhkZ2RIZ0hVc1ZWSzJhNkVpQ3I0QXJHZmxZc0UrazdJV3gwdm85enE0Y09rY0xsNDZoeXRYTHVLclgvNHlqbzZPY1hSOGdzSEpDTU5KRGVyT1lmWDhkVnk0K1JJdXZ2aWF3eXpDbWV6OUd5SFpFOXVZcklTYUtua0ZzQi9UTmFsK0p1aGVmQm9TTlRMTElvY0NsR2pHMnRoSENZckNEaEg1SFlvalVFQ1ZHa2syS0xIZllSVittTHBvNjArREJFQVViTHkwVkdEWWRiV29yV2Z4ZVowN3U0VmlNZ0VSSzhNRE9kMTlMRVI5eVhTR2dzNDdFK1dzUU1Td1RPaWZ1NElYTmk3aTVjLy9XeGdlUHNIZTloYTI3dHpHQnovOExtNy82QWVZNVFxMnJsQmI3eWNmakNHaFhWZEpUUzQ4ZmlxVTRCMXhDOEI0NEs3WDdXRHYrQWl6M1I3Nm5SYUdSTmdUd1lGWVlPVU0xbTdld090WHJtRDV6Q282M1M2S292QUZGNldUM1dmNWlYVkpPOFFHRU1Ga1hHRXdPTUw4d294Ylp5SVFieFJhVzFINUFHSDhSQTNwV1hNc3EvSWRHd2FVRnVsWmdGYTdCYlE4QnJHSUw0Y3JUS29hMWxhZUZzc1lqeWQ0dEwyRDVlVmx0SHM5aHhPNXJTdGhNNGJRNnJkeDdzb0ZuTHQ4SGovOWMzOGRKeWNqN080OHd0MlBQc2JINy93UVAvN1IyMmp0YjJQVkZGaHNGK2lVYlN3ZGo5RTJCamZXRm5FOEhPTndPSGFCSXFNS0JNZTZaTU9vQ0I0UFVjYXNRbzRYNHVuY0VtaHZ0UVA5YWt0NGREakd5cFVYOGFsbm5zZmE1Y3Zvclp4RmEyWU9Bb01LaktwcEpTN05VSkZUUEFDMUZmMXBPWFhJSjNLcEFrZ2JiUFJlVFBPd0tYWnRBSEFUUnlBWDdvWE5vQ0FyaVJkTUtmbzdnQmJCbEVBaUw5L1JMSzI0NkFqeG8wSWlRUkZlSU5SQTdNd3VRdWxwSTRpUkxnazNQTTh5WjNRaDVjaWFQOGdoMHk1eFRra2hvbW16dEVSZzA0SXQyaWlYdWxoZE9vdVZHeS9oVXovMzh4amMvUkQvK0RmL1M5amEvYTVPQTlZYy94QVFFZGdMN010U05vUlcyWEtzdjZJRURNTWFneU1tN01OaVVCWW81K1p4OXNXWGNQM2FOWGZDenkvQ2xDVnE2MHIzS0pVUVA0WHhaaHpNQWphTThXaUNSN3VQc2JXMWd3Y1BOakU0UE1Lam5SMzgrLy9oUHdBWlR2bHgvdExZVTlxaWFRY21SRTZxdGxHamFGOHQvdlJnbmE2Z05oUEtiYVhnUU01d21rS05yQVlIaC9nZi9ydi9Gb3VMaTFqYjJNRDFhOWR3NmNvRm5EbDdCck96czdERXJub0RPVjZDVjA1Mit5WE85MmR4L3ZJbGZPNExQNDNoeVFsMkgyN2o3cTFiK09CNzM4SDk3MzhiUTJwaHBtOXdVTGJSSzB1YzdiVnhmbWtHbzlwaVhOVTRHbzV3ZEh5TW81b1YvMEZRKzRlL2FkQXR0Z1pzN2Z3R1d4Mzh4bi8yWDRDWE5sQ0RVWU1oWkZEN0hBRmRXWWxRWGswMW5heWJDVCtCU1lwa3A1L09YMjJ6WjVQZFhaWWpKbE11dzVRNUJiRXEvYWZqK2tLTERyaGtKMWdiZWtSRS8vZW9aZkFJSXF2a1VrdnN5RHJDc0N3cUt0d3QrdG9yMGF5bmRSankzRDNyOXkxMi9BRkI2UCtEKzQzNFFBZktUY0lJR1QyQ2xNZTUxZGtsQWFXbFBJN0lTdVZaalg0bVJ3WmpBcWpUaFJBNUVwQjE4aWN5eGtsLzQ2SnhBS2dCVUFJb0RXRjFwZ2REak5vWVZHUXdGTUVoQVFNUVRqb2RuSDMyV2J4OC9Rck9YN3JvSXIrTEFwVnZnU29ReHBOSnRHQ3pkZTFuOHdSbVlIaHlndjBuYmtKeDkrNG1oaWRINlBiN21KbWR4OXpjSW1abkZ6QWNEbjFpTHZrZ0V5WEEwWGJWb2JlblpGS2FoMU40ck9JVS96Z2RQWlVzM3hNempSUmJ5dm8vZHFURXdCOHhJSExHb292THl6aC80UUtxYW9KMzMvb2h2djdtVnpHWmpMRzJmZ2JQdmZnU3JseTlnak5uMXRIcnovajNxWjBvaTluek5RVHRYZ2NiVnkvajdOVUwrT3pQL2hTT0J3Zll2SHNQbjN6d01lNjgreTUyM3ZvK2x1d1FheVZoMlJqTWs4VWFFMkJySEk0RSt6LzZBRXR6RHVDcks0ZGhrUytMNGFkWEpDRS9BZUJXQjlTZHd3bGFVRTVnZ05UVGloWVJaTmg2eUszUWgwaDhSQ2t2R01UbXZwb2VFNU9ZMWVFck90S2ZJWjBhckhNTHd0b0l6anJlbnQ5Nnprc0t6RTBWZHhFaUs2RjZmWUU5dFplSkhBR3JSakdKcXhwbm1JWUpoUmNMMWJDd2xzSGVaQ0s1bXZpZEt2dzNrMGZXMlhQVWtaODhtYTlod2o2SkpZNm5NZ2VpV0dsdzF0YUU4cGo5OTJBRVRid2JoZGExSTlFYmR1eTZybUVzdGRyb0ZnV09xd3JEeVJnenJRTEh3dGdVaThOT0MwdFhydUxLc3pleGNlRUNGcGVXVUhiYUhuWDFOT3FxamxzMEF5aDh2MmpJWUZMWGVMUzdoN3YzN3VHalc3ZHdOQmhnWm5ZVzh3c0xXRnRmUTJGS0RFNkcySDF5aU1mNzIzaDhPSUE5T1hLT1NFcHltdnN3cUFBV1NSTUpFYzFTbzVnL1QvNDBDMVdDamJCNm9wa1NjV3dSV2FIVDRrcTdXQnlROXo0TWVZUUN4aWViai9EZTVpNVc1bWV4dnJTQWxiVU45THNkMkhxQ2QzN3dBM3pqSzEvRGVETEdoVXVYOE55TEwrSHFOVWZQYnJYYXZpVnpZMHdiaUVlRzBKdWZ4NDI1ZWR4NDhVWEkzLzZiT0RvY1lQT1RPL2p3UisvaEI5LzVGc3FIZDNISlRuQ1JEV2JLR2wwV1hEbTNoaTZ2NHQxYm01REJPQzQ2VzFXb0pwVWppZGtLTEVDbjFRYWpTS2FibENyakthWWZwNFV2U0lZVnVVRzkxVEVqL2pibFZyYWh3a3lxeW5TQ0oxQlF0d0hVTUNsTkIyRmNJZHFySEFyZjhadE5jYnJkTnFWK3dQVUNrV0tvYlluZFJ6UnVqVm5yQlN3SkJLelpiVVpNRkRjTjlzS01PSjZTdENoVGFZOU1ORVRjekQ1RkhMOUZnSXZTREFHU2RtRXJ0VDh0ZGFCS2tEVmI3M3BqWFAvZWEyTithUmtMc3pONHNyTUZobUN1TERGWGxtQ3hHSW9QUExHQ3VqUzQ5b1YvQTYvKzVFK0F5Z0xXaG14YWwxcER3YzhUM3FmUHVNMm9Hayt3di84WUR4L3U0UDY5ZTloOXRJTjJwNDJGaFVWY09IOFJ0UUFud3hHZUhBenc4ZVlkN0J5Y1lJakNuVWltQkZxTEtHdDRTM0JISWRabXFNcVR5ZC9EUVBmMUNyeGtRT1ZCSmtrMmNkcFhQbEUySTlBcW5od0VMNXVPdkk1WU1WRFU2NU1haTA3S1BnNmtnNE9CNFBiaEFZenNZcllVbkZtY3hmTENERmJQbmtPN1pjQUEzdm5lZC9ETnIzd0o0MG1GeTFldjQvck5aM0hweW1Vc0x5K2hhRG1HcVJ1cnNvK1JKMUFCekN6TzQ3bjVsL0hzcTY5QWZ2VlhjZmo0TWI3MDIvOExKdTk5RDFTUDNlRXlHY093Z0t3REFZMXhuL1g1VDcrT3FwNWc1LzRtamdZSERxY3lCbFpaYWhOUmhyQXoySUdoa2p2K0VUV0NPVUxyVE1uNUtiRDlFbi9mRTM3aTRnMmdwckxhNWVCd0xNaHRBRnpsQ09Wb2xhekIyVlByV1gzMlFDTjJyMTFrSHpyNjNEZG9qQkpPZDNib3ZpNUZGQk9QUEl1UHlZRXZ4bG9VSG9jSzdEZEw3RmxQL3ZkWm81Yys4Y1Z2TGRIdFJ3RWhGQlNGa3RCcklPL0JvR08ya05KNUF0c1JQaDdiVXlGeDllb1Z6Qzh0Z0F1R01TVjI2aTNZdW9JWUp5SVNhejJHUWRrT1BMdTRCRzYxdmQyWGpZQmdqRG0zRHJRNzJEL0E5dllPSGp6WXdzSCtFL1Q3ZlhTN1BmVDZNM2pteGcyTVJrTzgrKzZQc0hObkN6dUhJeHhOQk5hMEFGTUE1VHpLVGhmOW1UNzYvUmwwT3gwY2JYNEFzUllzSms4YjhpZTZSTjJsdno0S2VDS2xoSXV0bSsrTllaVTVteC9oaXQ0dTRreExWV1N4d29BRGZGWG12WVdGdFJWZStld2I2Sng5Qm50N1Q3Q3p0WVZIajNieDVPZ0Erd2RqeU81akZQVkRMTStVV0ovdlliWUVubjMrQlZUVkJBODNIK0M3MzNnVC8rOGYvQjVFZ0F1WEwrUGE5ZXU0K3N4VnJLNnVPZmt2aDBPSjQyY2pCaFpXbHRGYVhQUnRuOGU0Z3JkRnVJOSsydE9mWGNEaTZpb3VYMzhXMVdTRTk5NStHOS85MFYzVVZsSWZyYVpMU1YwblNCNVZPZ2xRVlB2b3RTdmlCRmcyaTFtckZFWFh2MDZzNms1MzBaTE1TU2pjZzdxaG9DV25Md2tXN083SlRYb0xkVUFXN3FheDZzVjEwSVhFdU8wcEwzT1ZmUjQ5ekwwOUZSUER1QW9mbFJnWXF0MXA2TzJmVSttVWhwVXhGVGVRVDd5cmpMdDV3WVRFYTYyOVkxQWNjbFBnclNkWE5BdUpaaG1RWkhJU3FnTHhyMjJZME90MVVKUk91c3RNSHN6eUh2TVNJd1RqS1crVm9vc04rdzNDYlRYajhSaVBIejNHMW9NdDNMMS9ENFBCTVdiN00xaGNYc2JLeWlvV2xwWnhNRGpHMXFOSCtOeG5yK1A2TTlmdzRPRjlUTWhnODhoaTBwcUg2WGZRNzNiUjZYWFI3YzJnYUxkUStBZ2kxdGh2WUdoS3lvSW5aRUVJR1p1TWc2Mjc0b1ZMOUlDSStzUElBOHNFZzRqRllHYm9xa0cvQU9hbDkzU2JTS3RzWVhWOURlc2I1L0Q4OHk5Z05Kbmc0T0FBTzlzUGNmL2VYZXp1N09KeFhlSEoxaTV1TGhmNHlaLzlHMWhZV3Nidi85Ny9pZmZlZmdjcmF4dG9GWXp4OEJqZis0dHY0aXRmL0dQQUZEaC81UnFlZS9ZbXJseStqT1gxZFJlUHBnUTBzVU8yS2NNaGNEUWdJZXpVMDIvSndFSlF0anJvOW1aU0FSeDZkSCsveVdOSU91c2lzdjFnMDFTS0dpVTVwV2tLS2ZVZTZUQmJiMFdmY2k3Vlp3NVdQS2Uwd1NDTytJTCsrYVFVOU9wYjdaWG5YN3VvS1UvSVNhVkYwcjlMR01ORklJNkRHajNqNURNWXRUL05qWFVMcGVBNjVma1p0Y01wSkRtazhaSjFtNFpZaW9ZaThENER0VVh1VHhoWlZLd21NSnc5akxrc0E1RW40TVpqdGNjUUtCcm1XYkV1TU5Qakd1SkhuVllITGRxQWRLc0x6RUExSHVOUHZ2aW5PRHc4UXFmVndlemNQRlpYem1CaDBjVmpiVDU4ak1lSGQzRXdzcWlwUUNFMVBsTUxxbnJpRFN3WUtOcTRjTzBHeW5iWGw1ZGhTU1lnSjNUM05nS2NtRWEwSlh6T1pOb1Nsb1E5aFVERkFHd0FBMFZuMXZrSkNEbU1nSVZTY0tiWU9JR1JpTFo0TXBGbys1ZkVXTE5CTU1ZRjV1WVhNRGMvajJkdTNJUUJZZlBPSi9qekwvN2ZEdmdUZ0kyN0h0Ky9kNENxMmtXSEt5elB0TEE2UDRPbDFUTm90d3RNamc3d3JUZS9ncTkrOFEvUjZ2Ung2Zm9OL0oyLyt5dWd3c0JZdjRGYk5jVVJmeW9wMWlNektWMktud0w1dHBHSU1oRWJWQTRmMFZSb1hiNzVvazY1RnBZYmViL2h1aVN3TU5IZWM0TmNvb2FiZDhOOU03SUhRelJiSEZzSENqMnBZU0dsc2JxdkRvdHdPZ2EyVjhUY0lnOWNJaU13dmh4WnNIZWtJVThSRlc4SXdKRjM3Qnh5VENUcVV2SStKOGxHVGh4NkhlUDdkZytnRmVMR2VLWVdWT3cyQW5pa2x1UGUyNHhMSXJXd1V5Qmkxa3I0eW9EOXJoamJEZjlnaDNSYUlZazBUVktiRERzT2FseUV3UmhrTXE1dzllbzE3QjhjWVd0M0g5dDc5L0RrZUlJeERMaG9BMlVKNFFMZGJnK2RGbENSSW5aNHJJU05WMmRhaXFOQmFPSU5KTnVjdzBORWVscmlkOVVRWVI1RWs2Sk9hS3ZOSXNVQm9oSmsxbUdiQ0lFbTRacEdJSXppQStjSVNpbTlLQkt6c2tCTFIxS3lNTDZhOWdySHd2WDlaVkZnWm00MlpTQ2docUFDRlNVdVBQc3lQdHA4Z01lREFRNU9MTzRjamNEVkUvUzR4dXBjRzJlV0Y3Ris5anhhWllFZnYvMTkxTC8wU3lnNVpYZEpTRStLN2dPK3FwT0dkYTI0QThpTjFtcG5FVTlXQVg0K2Fsekp0dE1KN3Z2MlNLZW5qRThCTDZ0SE5sbkpMZkdwYVorblY3OVFnd2hzRlRHTFltVXJrVzdjTUJ6eFpIL3hkRlZTRldDUitld3BLYU9FV2k5cTkwbkJSMG1MblB6S3JESUJvVGpqTkg0c0lScFFpWnAzWDdSeVVqTjVTQVJXckRlaWNDY3dXMkRpcHdPV1V2SVB4MTNRUnNwd3RLWnFlQnlFbUNjT0Q3Q3lHdzl4Nk9HelVPRDJxblpIbE9lYnY2OE9SYlp1ZGwrMFdpamJiWHoveDIvaFVIcW8wSUh0OU5EcWRESFRtMFczMzBPbjMwUFpib05IZzJSQmJlRnAwN1d2UkZSNks3RXFIZE9ERk9mYVU3bUptWlZ5VE1taGhoMHNCWXEzTkVncXF0N1BZcWRVTCtEeUM0d2FFalFZbThoTllVUDdGQk9TeVZIRHBYWUp2MUxYRUUvQXFhcktYd2VMVnJ1TjEzL2lKL0FwMDhiUllCK1BkL2V3ZmY4aE51L2R4Y0hSQVk0bUZ0dGJJNnp1M01GUHZ2NkNrMmpYTmhzTGE1SlRPQ3lzVnVwVEFrdUZyV0pTc01mS09HMGEybkxlT3k5bEpibzRybDg0WWNsTDQ0TWliNHJ3NThWZ2FKN3lwQ1pmaVBQenlLUU1HM3dDOHhQck5vM3JKUXZ6U1ZjaUpYYzdBcDhIT2l6cGNFT2JpRGlTekVISW4vb1E5bENkRTUrRS9vTTlJaHl5QThpUHZJaWRlMjJRb3JJYVgzSEQ2RFBDS05aSmp5MERrOXBwekprWXRUakpzWnZXaVJkWmh2ZE5tWEN4ellocU9FOVo5cjFoT3VXOFBsNnBwUUtMS3V5b2d1UmVESko0azhMOFhoRCtIVEJzVUxTNllKN0YydG9hZW4zblJFTnFrN1ZDb0VxVVFZT0hhWVJRV1VFcGt2bW9pdmFmVjBieGVSeVZOdmJRdlZWTzRrbEJrYVJxQVVTZkFCS05adXZVWVZFMFVuTFZQNU02R1FXU2tXQk9FN0JJRWs5SnNwTjFSSnZBYUVSVW54cDI5OElVQlJZWGw3R3dzSUxyTjI0Q1ZqQTZPY0s5TzNmd3ZXLy9KZXFxVGtHazhJQnNURmp5ek0wWW1La1dRMmdUQXlpbVRQdHB5cmRTOFV3a21Yd2tHYnRWaXpxbFZTWm1abWlqT2F2cnM3eEtLTVlwa0lYYkJYRnhLSDJ6cEMza1JNM01Jb3lVTzdHb1Z0Wkg1QlZSbWhnYUhac0hkNGJSRDlXTk55R0srQ0piaVdNRlNGSkV1VXhJVjg4clJYOEViRVBXWU94ZFZLTWo3SGRmUDY2cEExQm9EWURLNHppVUVWUW8wbmoxQStaR0tKS05PRzNhK2JXZUFFSGhtREJWTzUxOHBwbEgycFZaZ1Y4TVprYXZOd3RUdGhzRzZ1NW5hdXZjaG9LS0xiSzlyQ1JtV0lQaUdVdndPSTQ3UlQ2cUs2QXA2YVhrNWhOWkdET242WGJHc0pTVXNFUWVTUmZPTXF4STV5VEd5WUxPbFd4NjJxVU5ScndwaW5mZ2pzOUorRXhrNFhRT3hyOW03WWxCblM3V3pxejdhc21kN2hLY2lPUDBROC91L2JzeFZEdGpHODQ5bklDNDBLckd5bGhsWXdSWFhTUWI5Rnd0SjFsd1JaalpreWJ4eEQ5TDFSVWpZVFpFK2VaSm9sZXpOSUJDeWFQaDQzc29ncDZrNmx2VWxLelFGa0w2WW9XZG5uVXNNYWhoUnVrQkgrS2toODR5QVB3b2dvUC9HMGVGV3Z4cXJNb2RTUjhzN3Vqa25ZTzhLOHFZZ21PS2VGdnljUE1ESWlEeGhCUHRGT0w4dHhXdHVSRkVvbmpZcVUrVW1JSkVsSGJlTEdIZHR4V0p6cXRPRVdhQTJYRWZSS2tWUldDcDlxd3pHMCtzNklVZ3VSMGtxNDNlbmRLVW9jU2lYSDMwak4rUmR5U1ZnTTArVkc4YzJpR29ZV09wWTZjbGdyYzJKbk1CaEFBQUlBQkpSRUZVVndhTDVHWVhwSmh2b2tJMWZiS1BIazF5aGxxclVXTTB2eEV2dkFyWHdIRUJ4Q3F5RXF3WEtVa1dqaW1CdUJiYVJTdnhsSTRDSHI5WTR1bnVSMmp1SUpPTUNSazY0MUQ2Ui9JZVVXN1lvVk9BQXFVNmE3RWtZMjVvYzF0U2RIbFNVdnBRMG1kYnFTUkx2YWo5VUgwZGV5dS80S2lWQ08xdXpSUU9kSkVjeWRTWlhaS2t2NkdrSXAxd0dwRGhtUEdZZzI5Q1hnSGxGeXVwTEhTS3ZiWnlLN0dLMjA2cG03UytKQXhHbXU3a0FFeVE1Z2IwVTRzc0tER2Z3c0p4MklaN0krWlFIbkVtaDBtL0d4U1JITTZ0QkppcWNZMlZZRVVkenhIL29JZzZoWnM1aVpRUnIwSmxZY2txU2JiRzc1T1hZQnhyQ3FkVFN1a2xnbzlpV0pRMnFNVWtsWnpTOEpybGFOYVNHSDNhck1VcFBqVUE1Z012bEZ0VE1yWlZHMDNZbUlnOGExUGlCc2ZSNW9leVFOZG1nQlRGc2x1UmxxSnV3aG1WU2gyb3p4d25QZEhiUW9GMmljUEE2ZDVJS3Uwa1k4V0tndk9hcVRxVVp3VEtkTGJGZEdDSVJrclNwQ1Z6UGRLcVNrVnFnOHJzUEsxQm9TbENOMC9aOFNlUGdMU0dpeXpNTSt3OFdXdkJ1UVFSb282ZEJESWx3MG1Dc0RjYVZha3BSQzZnTXExMHpqNHlwV1lsSWEvZVZDUlVWbFlJUlMweG02ME00d3psdXlhS0VFU2srQUpCT0tGT29nakNxUGtxa1FkM0FyR0dkSTJ2SFhUMWxrSHg4NUl5Z1FxQW8rdHBVKzZDMVZtTVVYN0wyV2FSV1ZqcnBGaEtwVndvUThHVUpnR05hWk5rSUpDWExHZUxuTEsyanRSSkpvMFdtREtqYXM5VlQ0TkoxZHVUbW1jcm9WRjJLSENnZENtekRFelpYOGVlT0NKZXFTejN0Vi84WjN6MDJWdk5JeEZob0lFNTFhcG1aWXgyelNSTm1xYW41TnRoNnU4bDJxVkpkazFsNm5XVVFjaFV1cGRrczN3aWVxci9CVFZ1ZHR4RUtiLzJVejJmWDRjRlpRd3hqNUdTY28wSnB0eWk2YitBWlpuTzhZTmpnN2x5eElDa25zb3BTMS9NWnI4b3Z0THdGSjdNbE1oNGtESlFLUXNBWXR4UEdwczJ5ZGdFQ0xJeENlSVlUekVEb2ltZXFKRVFaY3F1ZUhOc0FteGlZaTFsSnI2eEtyRWhNeUZ1M21rcVlxbWgwb3ZJc0ZXa0hoMzJJRStWbW9vZkV6TFN5REFSMGROaUQxZ3dhVGNJNUNlMW85ZWlVYWxJOWdUb3lKY3NNRmEwVDMweUVGRnRacXJLZkg2QitKSStUci85dzJwRmV3K2tDY0owRUoxV1VQb056dHFJOVZ2dmpveE0wdTY5QWNTQ3lDcVFrMDYvempaWGxVSlZXazJpRFdVaUlPWDBnK1RibDlveXloYjZWTDBRTitIbTV0SzQvNW1xUHprMmE2VWZOZitOR3R1WEV3UGx1dkJjVk5xQW9YeWlyYkR2NHl4RkVDUzBtK2toSkVVN2tSaXREY1V3aEE0U0NXa3BlcFJFZ1RuSWNXUVh1RDZtRnE4eVRObnFObEdjbzBFSEtYRU1VNUpqQk5FS3dCQ21lS0xGMFZ2UVNPdmVOL1RYbERZYnQ1ZUlWMEVycEp2VDRrOGhNRXI4NGZ2UFNPS2hSRmpKWFpva1kyK0YwOUQ5ZXBwTVRIRkd0VE9Ta28zbUluU2xOZmVidDFJS1IydHlhcFNsNHFtbWdUY0FCUnFuZTVjL1FjeVVBdkhpdFdZMXNlYUVYbEFLektCTXJweGpJMVpkcDdRTU9HNmtxYXhYbjRPQ1Y2WE5nRE5rQWgxS2JXTzBwYVBNMHpIblRJbjIwZmJQR2lzZGdHVDBhcjNJeGI5SDhOY1FrVk01d01RS2J4REphTUZCVWt3ZWQzSUt5dWFaMGFCMSt1OWQ0S2xwNVZQMW55ZWttRGcyaXowMWtJRVgvcnhUckR0UmNRREtrQ0xiemtrQkpmNHhVL2FwUVZwc3ZkZ0lMQ2lGSU1ZOVJHeWQzaUNMS1l2T1hja3VTbGZZVEtKRUhwTHExQXdtVC8rTGEwMEFJK1Nqdmh1N2RFVHkwMm5Ma1VLcXBzd1VxVkdaQTJMbWlaQjV1ZXM5WHlLZ1dNZnlOd0ZnSkZEdGw4WWIvTHc3M0lMNDhDRlNyUE9Pa1NLQksya0pPRGFHNmFGTndUSlRwd3dvam5zbFQ4Rnp6RHhTcVRSUmhXaFRpeGp3QThYVlNOZEQxYVFCT3dFYUxadXE4SzFFZmdOTnpTcURLTTM3WFVRRW5STEFLbXFaNjJoNnppZGtUQ1lDblFrL0VPVWFwSkgrbEswSjRKUnl2L2taT2VlOFRRV0tTQlFPU1NSRGtXSXU1aDRGeFRScXdRMVpiWElEOGxTcEJFeVJkcmsxdmhhMlVmTEwwYVBOTkZvUTlTRHFtMFhOWWs4ZkkrNmhZRW1FQnlaWHdsaFdTb25RNnlsMG0vU2NMcWpKbE1lZDJNYnBTSklaa0dRdXhvSGs1RFVLbnFDZlRVL1NtVWFOOFp0RUd5ZzlYdy84L0NsU2pZcGh6MVZtaU9HdEhFdnNOSjdTbm9ya2UrVkVqWTZzZzR5NnlxQk1ZcXJIbmZFMUE5ZUFrL2twaFExT29jdTVNNTNMTlBERmVmYk1Xa2FjUzRleHJGc2tuR1ZENkFlV3lmMmVPK1NDUTFPVHVPU08rdUIveUdGMnc1N3lTNVM1Nm1hbUlQNlBXTkxuc1dGekNoTFBxWVZLeWpWTEdrQmNibnlMeGdnMDdIS01wL2xjcGlsRFNnWFNKd01sSEtYeG0ySGp5bVZDNlhlUXFNQktYaEE1OElGVjVIZFh6Nk51Umd4R3EyZzFUODZCRFpzZHFobHpha282YVlGTTU1dzdCUGxIM244dUFSdWc4RlpHTlJFcVQvNklSWWVJTWtKQTR0V0h1V3NVTURWcm9DUnAxZWRXZU1DczN3ekpXMjBIT0NHZXJOWjZYUUZGVnlSU2hCQWlpdGtPWVZ3cEhqdUE3bGNwSHlkbElDS0YyRFNLbTE0bzlZVW9xaDBsbUVaa3V2Q2tOWSthS21vUUhxWXc1VURXZEthb1JQbEpIS211Q2hDRjBta0V2WVB1anFQMlEvRXJEUEhVNlpqRVhHbmtGYkdKWm11anJoVko4czBESlZhanhzZ0VlcFRZQlBhYTdkYzBOMFBVTmFERy8wOTE0a1JQUGR0elVPK1VPbHdhbXdKTnQrZDB5c1lCa2FtZmpTTmhKcmQra3EydHV5dU81Ky83cEREamhnNzFhSUpaU0xUYlU5QURYYmhSazdPdllReWlUSzRaeTVid01Ia21FMGZHbmtIdGdjSGFBRVhJTXJTZXVlaEhRbzZMbmJhNTJ0LzBpazhIVjZMWWhSaVpCN0lFblFHbno2aG10VUlVMnhVcmdvVTVseDgvbVV3YTRKYUxNek5zVkNxVG1ySjRJTk5RcmhnTEl5V3BiUVI3WXZpRE9HLzZsRURVd0ZveVN6YUpJOUtJQW9oa3VmYVIrVWVOYVlYNFRVMjVEd1hpallqMTNRK3B5RzdQZXhCQmJiMTlPTktJMEVyQzZsbEphYVdKc2lJeCtaSzRTL2tZUlBCTnZPOGtGSmd0YXZHcjZYc3dVeUhLMEJDaTVQb2NRRXlyWEhxYmFkMXhOMnVNMEp0VC9sTlBlRHFGd1RjMUVVQjJCSjN5UTFQNllVSVRkTS9adHFIeUxvZzVrMUFHcVFGbHZpZXFKNTNlQ2lPNGtoaTEya0JFOTZEU29KZW80UTFsWXVkOHR5VEZldkxnQ25rdEFqajBxTmFYbXVGVUlRVTErS2d5LzkrRlB3SEhZUlNvYVAvdU01dXMxSGJBVXAyWVhKRXlxN1pudGZpRDJHVnViaFl6aXd1b3h4TWNIdzl4ZEh5TTQrSFFYWjFKbWZwZkpIREhGTWFkbE5ibDZBWGdrbUJkZnIwZEE5VVFSRFZBUlJvRm1vYTRKYzBQdlJESFh3UEs1ZHlCYXlHQmZNT0tHYW43ZGcvVXdpLzA4TjZpZlFlSkdocGtUeHdiSHFLc1RsQ1VYZGVBQkZHTnBpVUtlL3R3VnFNNWF1Q1dOa1pyazZkVVQrZGxwODBGYXJKTWpRVXB1a1dTbkVrWkJvdWtkQzRVN2pGVFRnbk5mZXN3YlYxRENsaW5CcTA0UDltbmw3Q2t6ZWhwRzBqRFNQZXZRdk9tSmd0dW9pYU5QWEthSkljcFJoaE45VThpVGFwd0lHMlFMOTExZ0VaajdrdDZBNkJUWnBZNjJjcWRYcXdRVzdkK2pkcmxKS01yeHhPRjFhZ09CR09DMDdoVnA1MEZVQ25VUDVDaFNBV1VwbDRTS3NtSUpORnpYYnB5RFZ2WG9NSmdibjRPQ3d2enFHeU40Y2tRQjl0ajU0MUk3a01ZWmhBeFptZG0wZTUxTVJtZW9CNmRBSk1LOWVnWVZBMnhOTmZCMmZVbDNCa3NwckdrWkt4Y2Z3cXFWa1dRU0RRSTJZOEowQ1Rsb1NoaU0rMS9vOE5OR0loZitiYXBrMVgzTXpFVExkYm5DL1FHRDdEMStBRERtdEdlbTBkbmJoRkZ1dzlqV2pEV1JhRVp3MkNZMkJIbnk5dGpRQUdzazd3MTA3UDJTS2NJNFNoS1lCVXhHZ3NrbTN1Vm4wdmFkMTZ2Q3EyUTVOd2tBVUZtckE4NVZrd3VQcld0b3FjdWE5V0NUeFgyalora1UydUNobzVnR3UwSWYxRElhZVFHb3FmQ0VkTUV4bFA2Q3cyM0I5RVpzM0pEMFF5M3RMcEp5UjhwTDl3Y1VNZU55eUEyekJzU09LZkF4RUQ2c3lLUmNpd3hUNVNpOTNwTXZMU0FGS29YcG1TdFpZT0UyTCsxRVhYY2VQNTFQTldRbEY1VjdkN2IraHdGSWtLdjEwVzVzQVNTUTVEM2dDOExScGRxako4OHhNbnVHRVU5d3BuRldXeWNXOEdadGZOWW1KOXoyUVVDVEk0T1BjL0NleFVvNmxFQ0Y5SXlDZ0lpSnVQNmR6QjBVR25jdUsxbUtpSmFZbG5OUTFkcE5va0t5OWxyUVhSYWs4WE5HOWZ4OG11dkE4STRQajdHMXZZVzd0emR4T2JkajdBM21LQTF0d2dCb1kwS2hvcUcyYld5OXRERkp6TU1OM2orVVpoTGNVcEZXdldYRVk3VGhoamZqeFJwS1ZLUHd4aVFWZjlzRzR0c2VyNGVxbGpyM1hxQ3hiY29nNUFRQUpKczdyU3RYYUNOMjBhVjB2REhsSnhkTzkxaWlPZEZwSFpTSXY3Q3JnSklZaGlLbzZINDFYVGNjWWc2aXE0MGlkVjJHZ0JpeFhxRGh6b3VUQUdoaVpXRzJYb0lTUWk3bGxVVXp1ZzBZME1QN29HNEFBcXFIUytBVFhFeVRCSU5FZ0pwaDRTOVE0NENqcng4czFha2lid0NZa2R1RWovSGoyNHcrUnlNUU83MGwvQmVBZTJ2SEpHSkNXMVdJaGtDcXZFSmZ2S1ZxOWk0Y0E2ckswdm85M3Z1eXRRVGp3Y0ExdFpvbFMzL1FITThJekk2YUxNcUR0NklRTlFkcEIreVdURkhLc0dXbEU4QlRaV3FsQmgyK29UaHhGSms1YjlmVmJVN3U1blE2N1Z3K2RJR0xsM2VBSUV3R1F2Mjk0L3c0TUVEbkMxK0doKy8vNTd6UkFCQWRZMVNhdWZxN0xiZHRGa0hTYmt5N3FCTXdJYlRVWEhvQ29laVAyVno3T3ZFUkJ6VmUwSWFqTTZGVFFLWk9tSXAwcU5ZV2Q2cjlpU2ozeWttWnBqbmh6U3N5S1BnbUdpZEM5RWFsTzJwUmx1OGQyVER1TVNmRTBVVXAwd2hvRjQxRjFCaWZWRXA5ZDJoaHc1ZWUwMWlwQ2pSVU5wRUtENGNGQ3lOSkkwWWRXWmQ0QWZxNml3UUhVTHByY2RHNFQ0WVA4WUplV2dCYlpFUXRVenU1SVgzOEV2b3F2VmdGTWVia2ZkTmxJL3JWQ1hvUm9NMXJIaCt1bFJBWGNIVUFwSWFrNU5ESEIzdVkzeTBqeG1xY2UyRloxRGJHc3ZMUy9qbFgvNDdpY1pLN0crUU03NDhPaHJpMGQ1amJHMXY0KzdkVGR5OWV4ZXZ2UHhjSWtkUlBzbGdVZnNBSzE0QXFmQUhRaTVySlptaXhVWlpORkk2YjBETlE5SnpKRnVSWWloSUtyYlpHUHp6My9tbitNYlh2NG1yenp5REs1Y3Y0OHpaZGN3dkxzQndDNmJENkhUNldGOWZ3YXV2dklpNi9nVi9hRlE0ZDJZRlgvL1dkM0V3SWN3c3JtSjJhUlZGYndZMUdSQWJNQUhHT0RmcGNHcXljdGdOWHBQSkRUcFZiYVFseWxBMmFhUzBDVXF3QSszQTNIVE1WbHNpWmNDYmVwMEdHSWVuemYxSnVTNDFSck1PcjV0YVlWbFk2dWxZUWpvSVNObkNPeTJBMVJiREFjbTNPZnZOcy9Gc09PbHNtSG1hSkREd0xyRklQaE8rTDVlb2o0N2VRRjVnUXlMS0w0aWk3cDZDdDRxM0lZTm9JZ1psZ1F0aUpUckZ4aTVRYkVZUHp2cGtQLzZBNzNldE12cHcrQ0lISTVnWVJVYjZRUWtPeEtveVNqYnR0VzhQTE5wMmhQckpEZzUyTnlIRFF5ejEyOWhZWGNMSzlWVXNMOXhBVVJoQWFwZElGTWFSVEJoUHhoZ01qckM3dTRlSFd6dTRlM2NUZzVNeGFoaFlMakNxQmNPaGkxc3o4V2lIeHM4amc4NHFJQlNrc244YStRSHBHYUdzZlp2dUdaT2MySHBMOW1qMElxU0VMS2xpQWhuc0RSbDdIMjdqN1Evdm81QS9neUdMczJmV2NQUFo1M0RsNm1Wc25EdUh4YVVGRklYeHFVdXVJcnorN0JWY3Yza0ZvK0VJajNZZTQrNzlCN2gzOXhQc0hZM1JtbGtBbFYzMERjQ1Z4SVZKVXpSYlBXV256Q0JMclRsbGNJcklIM0M4QVlvYWtUVHBPY1hiNDY4WTh4RlJBMkZyQW9pbjkvVTAzVmhQQTNxcW10Q1ZUUGE3Y3NyWTBEZlBSUzJaRWdpMUhyMEpaVzFBS2ltVTVSTWxJd3JyRFF1Q0NpdFFlU2xZTTFIUytGdEtWTXk0a0wwZUlQbVpTNVFXQitLSTYvczVqclEwMHpCV0phVFJmOGxkWFZtbWZSNlFyTWZKTDI2SjVwaEp6S05EUndUSjJJQWtsRlFXdytFUkxwMWJ4Y2FGQzFnL000ZlZwVVYwZXoyUEw5WXdiR0NNQVJ1R3JkeG5QaDZlNElmZmZ3dWJEN2J3Y0dmWEJXb1ViVlJvNGFnaTFOUkQyZW1pUGRQSHdzSXNEdTlSbzZSbE5YRmhwVnZYckV2S0RDcWdaK3JCUW94WitRR2tLSGRSU1QvaHVhQVlmSkRjY3FMNVJpQ0orZFBzbVZjK2pWRjNCWnViRC9GbzV4RmtNc1NUQjBQYzJmNE82RXQvRHFrbjZIY0xYTDk2QlZldlg4TmYrNmszMEpucHV4S2NnRTZ2Z3d1WHorSDhwUTFBREtxcXd0N2VQaDQ4Mk1IeVQ3MkMyeC84Q09QaE1DNVl5Z25sYXFxbHVQcGVQdDRFalhQdUNXV09TN0VLbFNiOStwUitmR3Jobm83UDAxVDZiODRIRUFXWU4xMEZ1Y210MDJta1dwaEYwekJpU0FRdGFoWDRTUEdoMFJ4SG02WDZRaWpQTWM5NEdPSGtVYnVPcE5NU1FxaFJaNEx6bk5lczBGWlZHVmdmWjBUeDlLblY3TnpydGEwS1RCUmRGb2tTcVVpaWhBS1FXb1dkaXBZQUl6TGNXQ3dZSmlucVl0d2U1YlpjYmhhSGN4c2IrUHpuUDQrcXJsSFZ2c2UyRmRnVXFDM3c1R0Fmang0OXh1YjkrN2grOVRJdVhEaVBrK0VKdGgvdjRkMjd1eGh4QjZiZFE2ZVl3Y3o4QXM0c0xHSjJwbzllcCsxTU1WQkQ5cmQ5NVNNTnlnbEY2V3hTdW5vL1I1RmNFQkx0MjA3aGlTZXVjR2JvRWMreFlLQVpOczRJZzFEcVhTVlpxaXd0TDJQbHVkZnc0bXNNbWxRNEhCemcwYU5kYkcxdVl1dmhmUndkSEtJenNUaDYveDRlM3QvRXAxNTdDWjJaR1h6OXExL0g0MGU3dUh6MUtqWTJ6bUorZmhabHUwQlp0ckMydm82MTlRMjgrdXBMc0pPL2ppOTk4UTl4OTk0OVZZN1hjY213d21hZ1hLenBGTWVmNkhDa0RFOEZVS0dxdWI1TmtXNWpHMFJQT2Rxei9JMFF1ZGVjUERTMml0TTRCdzFyRlUrREpxRHBjNkVxTnlHVjVhamFrMktzM1dhbmhDSTI3WVVxYWpoU1F6WDFtdkpjUGxMUlJwS0JVYUt0RDZaTUpVVDdJa2FwcUhvZE5URVFQd1dJVVZXU3p5c2lPY092MEtTVWN1OCs4VEZRSk1rK1RFc25nMEZKWUY2RmlVUG9IbGtyc3J6SG9lRUN0Yld3dFdCNFBNVGp4NC94Y0dzYjkrOC94TzdlQVlZVEN4UnRpRmhzYkp5TFdBUnhBV25QNC9wekwyRm1ZY0dSaElnRFVvdmFiOFFtbE5waUc2TW5pZkZxYWJxU3pDNUM4Y09xQnlCbC8wRjAybGlLRk1DbzNUN3lFV2hPOUdxNEZFWXpaUWNJRjYwU2k4c3JXRnBkdy9Ybm5zVmtYT0Z3Y0l6N2R6N0JlMS8vTTJ4MENrY1dJc2Ird1JIKzRJdGZSY0Z2d3BCZ2JYa0JWNjlkd2ZYcjEzSGg0a1VzcjZ5aDNXbjVDUWVyeWxVRDE3VTZVa1JOZDlUR0o5VGczU3J2L2xDSlJyOEphWXp4Sk9acVRtOG11ZzZoSklmT1dyQlRBZ0I4aGNGSXlkMUUrcENWYUVNcWFneWREL1J6R3JJZVVUSjUranNSaWtudHQzQ2YxWnBSZ2lnNDVUWUNEZFZVSUhxbGl6NFpuZjQ2WFhuajEySml5NUhTbW1WNjVyQzRiZXByUlBLU3lLMW5iVTZpNXVIVWlFa1M4Ukpsd0RCSGMwb1N3U1M1WDhUeVByWWVSRlBUMmhDRjVTK1hxMHhpdEpuNzMrYUQrL2o0emozY3VuMFBCNGREMzd1WEdBdkRvb1dpMThmUzBqTG1leTNZWUt3WnFNbWxRVzltQnEyeTlJaTl5MUtvUFQ3QXlrazJDaFdKc2o0M2pCb2RlODFOUk5oWFBEVlpkWUpKMUc0RUxZT1ZYS1NsWmRXU21aT2tzV2dFMHpJL0FzNDRHWUxhUFRkV1hJc3BGTjJjaUJsejh6T2dDK2Z3YnJRTE15QVltTEtMcGV1djRON1dMaWFEQXp4NVZHSHo4QmErK2NQM3dYYUMyVzRIRjgrZHdlVkw1N0czczZXY3JZUDdGQ1dqVzgwWVZEMjROQmdIOGNmY3JEaDVKSHBjS2p3SVRQbllQUEQ4bVZMRkdiVWNsRnljUXlVWm1hRUU1NU5CMnJmQkprcTRiZ1dVcFduU2lDaFpmVFJUNHV5K0pTNmR4Qll1ZUhnVVZiQzlFbWVuR0ZSMFpLRjh5NXFzUTJxTVY2Yk5DVVRGSENVU1JlQ1M2NzVLZFdocU0wa3d2OS9ObW5USjREdWdYVERFUjExbGJnTWNOOERhSnR6Q1BZUUpDWTRWRHFFUm5HRVRpS1FhTWZHN0tLbVpNOFRsL1gzdm5mY3hrRDRxczRqKy9BSlcxdGN4djdpRStia0Z6TTcyVUJRRnFzRVRrSDJjNUxvV1BraVZraVpEVlU3d05saFdDMElhRDNQTzVWQlNhQUpZN0hROUtza2pMNmJjVUQ2WGhzb01US0dqdnBRZ3lVWmdBVHZSTHJSTmc0OUVWTExSUktUWDZXQlMrcEdtY0xMZktndjgvTi8rUlZUVXd0SGhBSTkzSDJGcmV3ZWJtNXQ0L0hnWDI4TmozSC8vUHQ3OTBZL3gyZ3MzcG5BUHZXbHhveCtYaHFkaklvNHA5aDRSbUwwbkFYRWtUaVdUVFp2eUxTaFI1WlBQU09CSm1HZ1NRcVJ0NlVrWnp3cVlPUnFKa01aaU1yWnN3ekVvZkY2cmNMbVEyNkd5UGxtUXhzWktPVmhZS0tnOGdDWVMyRjRKOVUrUlQrRW1pYTZ1aytReDBuQkZEeFN6SFMrTkx3U0pydVozUnlVMGFXZzU0L2ltYWNGRVRKR25iK05ORk9UcWtGVGlXMnZCUW01REVGMjJxaFpIbENvd2pJQnM0aVBRcWRvRlFxdlZCcGtTRlhYd3VjOS9BVXVyYStxMGR2K3N4SHFUVThwbXhOQTNPcmpZYWxscWNHdFcxSU1rMWJVNTMxc2tUNjRsQjUyU1dLQWhiMHE1QUpyTm1UeitFck9XbzVhUVlqSVJxVTBLeW1CRGNULzl5REFNaXNORGI0aFFsb3hXVVRTNHA1ekdqblVOMDJZc0xNeGpjWEVlVjUrNURoSEM4ZkFFdDI1OWpLLzk2WitnUjBPMFdtVTA4VWg0aG1SZ0hqWDBLK25qblNZb1N1Vys4YnlTYkFnWHZCc3pJb1cyY0daMTZvYjBMY2xPWTMwclNFbnFrNGVBbHFOejJxU0ZsVExaTDNJV1R3eVR6UGc4UnZpb1VYRTZPQWxGU3JteHl1SllvdTEzWmlqWlFCRERQRGhTU3RuN3hNWCtQVVdLU3lQaklGeVhPTmFUNUVac29ReENHNlllT2VkQ1dYMkZUNWJicjJXcXIzUkxHUldTOXg0cCs2SFFRbHBLdnlIZXZBTHNIV3JqUTYyOTRrSUo2SWhKeGhpTWhpUHM3ZTZpYkxkUkZDMFlvL0xhckV1N0RlQ3Jsc0l5RzE5NVNEUnpwR2d5VWZuOFFjbThEVVMwODdFYWVRVldtNFpYQmNxekliRUh3eWtuUHN3eVQ1Vk42a0pwK3ZZRkNTOVJOdEkxZ1JuaXYwdXpSemFHSGJGSmtYdUlVczVmTUxlUXVvNWpZZXQ1SllWaExDOHZRa05iSkw0cWl6d1Q1UmpVTUVnV0ZhMnU3MSs2aXB6K2poTEhJTnl2b0JWZ0RRNGpoYytTWndDbVZhY3QwenlQbXBYNWNrTm8xSnhINUo4K1BUT0pMK050K3BWVlhaakdjVllWNm10Z2xScFF6M21Ga0ljcTVQWkRvazYvRUVpcG5rVGZ3MHZtbHgzam9aVFVQcWpRTE9HVVlVZ3UxUTNHRXh6TFZHWGNMYVNZbkpRNTZsREdoMDU0UWJDalpxR2N6VU9wcExkeG5PVnB3cEpjZzhMRUlKTTR4U3dWaHZHT090YldHSjZjQUhJTUprWlJsbWlWSmRpVHIwaGNSUko4NllOcFpySkt6OGw5OGM4cE53d2xDcXIzd08vUHdjRncwbW1lcDBEbjFLbERrT1BjeEgvbkZBZnZKTVFwTVNlbExlY0lkT2lGcmVUM01rd1J5cExkcVozWTFOcEpIOGxlMVlXS2hJMCtxU1dkQ1l3aGgrMHdVbm11bnlWdXlHVkQ5RGFyUE1xTXRLUG5LY0U4Ukt5NnpybEpxR1NuS2pWMGdCSzVGYVJ5TjhJbVJXcGt4K0xHM0FxQ3duVDRHRExEazFSMGhjcEI4WEpKby8vNkFKU29iZ1NBUWtSeUR6ZkYvZEtaYzVweEZKMkF0V0ZHMDR5RjFTYWdDa0x0UHNBUmlKTGNtWlFvQzdtbzlSZW02WXBCR3lNa0N5OGYyS2lTalNSVEZTR0tWYVF4S0hYcFFLS2lyeE9ObWRCWVYvNzBzRllhVE1Gd21qc3YrNkM2bTB6R0dJMUh3UEFRcTMwYks1dTZ0ckRXb2lnWWJBQmJKZXVzbU80VFcyK0tsVW9jUS9uQVU0bUt6VHd1TFFLWXdhVTVvd3BUbWhybzZ5bU5HVGZJajF2VFZwTk85MlNVR3NoZmdTOWlmZjhwZmxNMGh0QXVTMFhkVFE5MkdxMktzalFYeGRha0ZPU2lWYUZaejV3N1EydEFRRTd4R014MFRFcHdSdjZVbEVoQjk1RjMxbFdJd2FZckJuTm1JUi9hbkNWOVZzQ0NUS2lVSlNyK3JDU3JXSTBKeEhGdnNKekx4SWljeUV1S0ZScGFCb25UREcyV28zMGd4RVdEWmJ1TlVDYndva3dvckw0Wm5TWXZsS242VzhzWktacHhKcDJtelJJODFJNU02Y0ZsQmR4Rmx5TGtHNDlJem1qTUFDdlNCWjRxaGF4Q2RxTWpweWoxRnVXRFh5Q0xsZEtjYktFcGttS0tjQ3FDbnp4Rjcva1FpaEsrU1MwV2xkUm90VnFZbmUxak5CcWpubFNvYkIyRG02bEJVcEhtK0VsRlZpV2YrMlEwU2tLbmNWRlBONDRVRFl6NmNqeVlxR2g5Q0RWbHNHbVNaWnRLTldjbGdFNjdqQTRqUk1pMUlXb0JNUkZxTkR6NzlDQ1hmSmEwQ3JSTnhCL3QwVTlUUnFkSzJhMCs5UFFvTDdMeWpKb3lzcllaMXp4KzZJVHZOQ0VMN2JFb3dEc0VXSWxIVlRSYmtOS2lKcGFZeEtRenU2bmhaQlJEZkNud1dMejRMUFZwU3NBVm5LOEZCVk9hR1dwZ0wrY0VVSzc1YTZUaFlPcjBRSXpqMG5pOE5MU0ltU1U1TjN2cG5EaEJpczZLcDJtZWs4ZVlwNldxRHlvTjhvVnlhUW5mbDdWaUxOSjljMnNUMXNuQW9qMzNLRE5zcE1aY25FUFpSU25jc2ZhRWt6aWFWSUVvWmN1Z0xBdUlXTlMxeFhneWdxMHN5QWFPZnAyc3pUSHRmNmZwd1phY09DZWl3aG10TFpUS0FzbWZxVVI3VmlFc0VRL1IxWnlXY212bGtFSzR3YzdCcWR0dXhYQVZWVUFwQVZrU1QyV0lmRVAwa3ZYSHdzcTJMdmtXcE9yVE5vUlJRV21uY1JNWFNwb2g2NXg4Q2FPZWhXUktTUmtyMWlaTk9KTG1QSUJISnJYRXF2ekpGTlVLUzJ5YWVBcWF6RDVKcmlna21RbExVdGVLMGppbzNkbmZnMExEa0F3b1I1ZFR1NCtuckQ1cUpKSlNJeDR5OVg2UWFWR0Vwdk9lcXBmV2t1eUdWU0JSN2hMVDlEN1JHN3QySWs2MlllbDlyQWlNUUQyYXJFWmtsS2kyQWUxTzBVYk9rVWR4R2JMV0tjQXdsQUlyYkF3TzhlUE1PUFpUNmdnaUVCc1V6R2lWQmFRVzFKTWhESnRjU2gyUlBrbzl2dktqenhYaUtlMDI1aVNRQ3BJTWhDeWtxaVVzYkYyQ3Nub2ZWbWk2anFMV3B3OFRvZDBxVTdLUzlYaUszNlE1Z3FDVUpTRlRGb2doa0dhb0NUVWNldUo2MEo4amxOYUpLeEdlQjJ1bmJkQ2hTSEdpTlNUVXFIUW9CYlpTYzNOU3dMTkdDYWM4ZlVoWEdqb2ZRMUhkZlN1WXdhaE5pbUEyTHRUYVlqVjlDYzhWSTl2MjRoZTJnbXo1eXBTb1FpdTB2Y0ZsUms1QTlMZlQ1YVBHQmpRWGZkcmhGWmhLNk5GZmxsVmJRbE9UbS9oZ0JvWWVxOGl4ZkxTWWVueFFpZ0duQnRVM2FQcFpDWTJjTWFyZWhCVENxbHR2dndBTDlzSFV2bjhNSG91azlPR09OeVhSdURLYzdCa2JrUUFVakZhbmcxYTNrNUI4Q1JRdXRlaXNSTytEZENCelk5Tk5UamVhckpKYU45VkNnUlNRbGdlM2tLNGMxS2ZRNDlLU0daMVd5eXNkcVdtTU0zMDlHMGk0VmJSbUtKTkwyd2lDMVFzeHRpa3hpajVWa2tiUmdFbVpvTkFValZhbWJiMms2YzlIMHc0OXpVd0hWY2lJVnFaUlF3R28xNXpvaVZkeitwWHpjdUttclpLalpjcGVKTWNJd25vb3BqVktPVnM0SUpteEQ0ZHorN1hLeng0TjFSazEyNnJzQ2RGWmNXbW9xR2ZxcVo5RDh1ZG5aSDFYWFBDUy83ZG9mS0RobnBvbFhjY1RrcVp0enJKN1JGbmEwQlNSUnRmTlFobUs3Tm9hem5KVzNJSXh6b1JUV1YySnB4S0hERVNyeUVueHFnVHpFV1pkcCtkQVVKUXFTMlorb2Y5RUpKQzhTV1VDaXFKcFUrYjFPSlZtcTlCOXNma21yZlVTRVlsblI2aEpVZU81NFJ3eXhxRW9KcHhDOW5WS2N3VHFiTVlyRVdWaFpqTVdTbTV6cDZtNzBWc2l1MjQ1SnlUeVdtUTZmRlhuZ1o1YU1JdEcwSklXaFRMMFhoTm5Vd0FKVTJNeGl6NklFdGt1WUVRcGVGUUI3NUliaEVyR3l4RVU5QlF6SVRUMTc4MWRMdE5XcXlQVGtWRnlBQUFnQUVsRVFWUStIcU9TK25wdWhGeFF6b3VPR21yVnd5Y1BkY2tNNTV1U1MyR3RBeEFQMExCekt6N05RTDA1cmZCMDJ5d09reWh2eUVMN0lkTytxS0tqcFlNVGJSU1RlTmcrM0V6L01wYTFLcEZpK3hWQUhCc2VlTkx5MVRSM1puWUNINnNwbm1Ha0dCaVZ1dktpM0RNbi9Ud2xLbk5HUnNxajM0SnBocWpUTElyR21JQmFnZTBxc3NkRnN5WGdOdFlQQ3RPVGtGc1lTRjROdSt1Y3UyQ3psaTdMcHBTUXJwVENVME1YWUtrUm1jVzVDVXdUWk5TbnRrVGNTQTlXSkl0b1M1YnZ5ZGRpNnZEWDRHWm05WjZ1SXpjRVE1SkZ0U3NXcmlUTVNxWmt4VnJUUW9rcUg2ajhvZjBKcnNERU9Vb2ZYMWlVaDV3MEFBaEJJNVk0c2RXMFo1NGpMZW1RUlU1emVZOFNhMFNkbEkwMFZFODA3WHNtdVpRWWVmK2o5eUxSb0ZUanM0ZVNLRTBjVW9TejlyVlBOOXl6SGttRmNEYWFtRXhiRWg0TURuYnJUYU56M1crUzZtMGxKdGpZRVBRZ2J2NXVtSDFMNG1nMk5zNzJHWm5tVFZKdDdhWU9ObW9KUkpwMjB6SmxiMkhEd3hLc3pFT2xBbmJPVFdHejBUTVhhdURBWVVQMGNlazFnSHJLSFNmb0VMeVBvVXczS3RISXBRbmdpZ2VQS1ZtaUNWR2VFS3hlemFKUmlnUHErWjNHbmdnTkhVR2pKYzFtLzhFa2gwakZ1eW5iTUVaazh6VmRnUElOSTN3M2p0b1pVc2svQVd2aE1HN1EwbUZxQkx1U05FRDhKbGppZzBHMHlWRjh1UFZXM1ZTS1VkTUFnVEp4WkdyWWtjZ08xUFJOcDhiclVrTlJscnV4Tk9uQitYUk9Fdm9iSHZwZ0JVNk5zdFpmVE01RU1XbW1ya2xOZVRpa1pLWWdFaCsyTkMwUUJUd0dycmZodExHSXR5SVRwVkdRakttWUVtNFpGa0lNNC9VSVF2b2U4UlE0bGxjNGFzT2lWQVlJTkJFb3BDeXArK0FYa3AzeStnOVh3V1RVc01RMlpCWFJUc2t3aE5JcExNMXVtZkwwVy9FOEJqVll6NTR2Vmd1VlUxY2JkMldKU3RQR1VhRjJHVkhjKzZsc0N1U1QzL2djMG1rdU8zNURZcVVsQ1orcnNYOVIwL3pqcVVHZm1QWWUwQmtMVFJPUVRMQnh5aHBWaXp5MDdxR3QwUDFaWVR4NnJlbVQrUWduSC9Gb2VueFRteHlSUmpWYWl4VkdFOFRJV0hqVXlBSVI5Y2VLZXF1TUVQbTBka3VRV3lvcE5EVG1zelZwREpRanVsQ0p3Q0k2MkZMeVdiVXI1aE10VStNUHNZeGxiMnBCMGNvNjlPeVd5Vk43L1UyM0RwRzJOZ1dOT21UYytROGFmd0lZY2tJb1U3QVhxcmhIVDJpYW42SFRrRVBzRmFucFE3aWhJdFRZTFBWMGdSUmVva2RoK1ppS1JKbHhSRU5OOG41MEZvV0JvNWNINE02ZndyV0M5YU5ibWVybFNmSkZsTWhHdnJUM21BbGxQWC9UMVZoUlk5RU0zSlFHRnBrcVg5SDAvc2pONTZuRlJxcjNwc2IwVG0veWxNOWFjczJicW1TVExrQXJjMVVjK1NuNDRsTTNGUVY0NmxGcCtJRUNFb3k3VkJ1c0doWU5uNXlxcjRsdGcxckx5aHA3aW9ROUpiQnRrakF3OVVXREdpNFNnWjd5V3RsdUtVMTBOcFZ6TmdPSzB3NXVRZEVKdGhsV0hkeVJwSEh5NWFpelg5eWU2eDhGVE9RazBjR0FnNGhBcFhHTGc1TGtXV3lGMmdkSWlrOE01ckJ3cXdsa2NvS1Q0eU1jYlQ4QTQrVW9FTkxxTjYxQno5MWlLYlpEMUV5MUpjbG1ydmxJVmQ5MG00UmhDcXdTc2E2ZFV5Q0I2LzlkTmZQbzNtMHNyMjlnWm1FSnJVNFhFeTVRQzZQeTl1ekVCclcxZmlTYXJqc1RPYndrK2oxUUNqWWppV2xGNFNrVlN2cVVwcU9CalJzY04wSmE4Z1dWOUE1Si9tNzlobVlicFRnMVJFYzRMVEJibmtMbmJUQWN3a2FXYjN5cWRXM0VKQXYrS2x0eFRJZHJpY1kwMG5vcmFPb1ZHNW4zaWo4czB3clA3RUhJVk5XS2MzbGE0cDgwU2hvNU5aNEptY0hJNlQrUHY3S2tvbE51U05DZVpMeHYwdlF3Uk9ETEVqVXVTeUxCa09RbHA2Z2ZyTDI4Mm9vM0h5V0pTRGlCVUJpRzRRTE1ISUVaRnNEWUNseVBNRGs1d3ZqNEVJTzl4OWpmM1FhTkJsaGZXY0RaOVhYOGpaOTVBKzF1SjdyRWhrMkZrYkljSVlSYTlibVJHcTNVaGlMTjNoOUFCb2NHeEo0aHVZbHh4aUNNREgxR25teE13TkxLSXY2ZFgvc2wzTC8vRUxkdmZSK2JEN1l3UVluNTVYWE1yNjZodDdDRVltN0JmKzhxdmgvSU9MNERHeDhHNHY2cVVzOUVEVFRzTnJRVlNZT3RxbzVpQ2g3b1JOTldYYUxBdmV3MDk1dFAwUFlMbnNiVVA5MzhTNmJPcE96UEdhbHlKWGtLTHYvVUVKR25MSUJUZnA4YWgyMlJ2YUFDd0dScU82SGt6MzRxRWFpUk8rYWwvcWVraVoweXkzektERVVqeUtSNTRNMzlUNll1S0QzbHZkTGN0VTQxR1VHcERoVndJOVBvS2pSUnlCT0JTQ1hwUUNIbTRqY1NDNDhGaU1DUWdTRkJZUXhZQ0VWUndEQmhmSElJTXpyQXJXOTlGU1hWMkZoYnd2bXo2M2oyK2hrc2YvWjV0RHFsODVtM0hxUzE2Um93bTdSZHE3QlZ6YlpqY1orVHBFR3JqZTJOVElGZ2diekYwS1RLQkRpbTNqSXBRTFgzSUFGZ0E1dzdkeFliNXpmd21UYytnN3F1TVR3K3dvT0g5M0h2emozY2Z2dkgrTzZETFZnVUtFZUhtQXo3cUtzS1pPR2RnWnhzMkhwUmtFSmlZbHhYalByTTZtSFNPaWcxbldGRjFCSTh2YVNVUko5VzB5Q09xc1VHUjRZNFA1UnlKZm1waXo5am9rSWkyVXBiNWYzL2NmQ0FQRjVNSDQ3VU1QS1FCdlNTYlFDNmQyMEdFYklDY2FUQkl5ZVJYT3N2YUp5SStncmtDeFZOeFJNaGl5bk00R3JsVkp2Tjl4c1hvQkVtQ3p4TlZrbUpzcEtHdVNxMk83WjdkU0tnSkVkdGoxOGs2bXVRc1FiVm9nMHRnSGdERkNzb0FiQ3RNQndjWUhmN0FSNGQ3K0hlclIrakdvL3g4dk0zOGZtZldjWDgvQUw2dlQ3QTFndEVVZzl2R3lFUVJPenR4QmdjYmM3OVF5Um9GUEkyTDFkSit6K21IbCtySEpLVHNtMDhpRGJScmNpQ21WQVFZVUxHMFdmSlJKMjZpMWtMWTBlWDQ5anRkM0hsMmhWY3ZYYkZqUzVyd1dCd2hNZTdUM0R2emwxODhRLy9DRVZSWXVmeFBoWXZYc2ZjK2psMDUrYkJ4c1JjWTBzTVdFbHFRR1lZTXZFbUJkL0hQSlRjT2VnRU5xWjRvNDhzU2x6TmZBWFNxSWVvTWR0UDFGcHBQb2luZ05kNUphemNxTU9NaFZUV0k1SnFjS3JXYUFLTjJmUTlRZGRUVlRKTi8zbGhvVk5FbGVVem1zNnpPUTFSamNZak9TanVySktFTUNMUzREUXJjODRtOTF3ditFd2JrbVBJTGlXSUdocUE1b1gxSTc1TXpxU1pob1NNb1JOSFpKUWlvRlYrSGlTTXFxQzA3MmxLVDk3TkI4UW9DT0RKRUR3Y29CNFBjUEJrRjRkN2o4R1RFeXpNdEhGeDR4eHVQbk1PaXd2UG9kdnZnTm5BV2hzVmhZTGEwMmNkVGlEUmdKT3owU2dFR0k5UGNMQzNEeVpDYVl3Q21Ud3lMVm9RUmJIM0lSL1h6WlJZVDlOVlUzNUtSbzU5NUQ3NENnZzFDbktNUDhPTXJRZjMwZS8zME8zM1lZcDJUSGEyU0Nha0hKT05MQXdCOHd2eldGaFl4TlZyMXdBd3Fza0Urd2RQY1AvQkZtN2Z2b04zdm5VTCs4Y2p0T2VXc0xTK2dZWFZkZkM0Um9mZ3JyZVBWNHZBYWtMdmtxREp5N3haRXEvQ2swbmNoa0RLdDhBdjhqcTRWMGUxWHk3SmFjNmRSZklnM0RTdnoyZ3VlZlVVVmJHSmprWElodituVk1ta0o5UjV1eTNOd1hTcVhtMUQwbCtrRkN3NkJUV2tCSFRvY1VqZ2QwUFJaaVdmemNkSWRVVm5pMHRQa3RmNmxKR2ZLSGVnV0JMbFlKYkdycktBUlVvMFpGQU1pWXJjOEZBZXhYaHZkYnBTa0JCSDNqREgvRGtXTFI1d0o3R2xRS1ZOMmZDd05ZYkhoMmpMRUpkbjJ6RGJIMkJ0ZFFVclYxYXc4S25yNlBVNmFMVmFjZFFZZHV1NnRsbkpacTF6d2hGci9jZ1BxR3RnTkI1aGNEakE0OTFkYkcvdllHOTdCK1BCQUZSTk1OZnRZZno0TVlxb3JhRElMak1SMjdCUnR5Qlo5YVJ0eGxST0hrN3BoVVVnVE5HbG1jU2lJSUlSd1hCN0M5MlpIdjdvZC80SmJHSFFYMXJCMnNaNW5MOTRDZXNiWjdHOHNvaitUQmVtTUQ1bjB5YytFNnZUMDJjQWxRWkx5OHRZWEZuQkN5OCtDNGpGeWZFQUJ3Y0QzSCs0aGMzTnU5aSs5eERyUEFUc0NJZUhCOUVYZ1RJeXU0M2NoNGhWaE1TZlVGRUd4emNWZ2ViTVVTaXg1Vm14WVNWeDcxTVZLZzFKY2VLMnhBa21Jd3Y5UUJnbGlsSmQ2bXNkWExpRXN2RGNvTEFWeXJseWNRenUvNE8xK1ZNUWNHbHB2WkJ2QVJxN0I1bzhBa216Q3UyemxpT2hVT29hVXFteDZiWFl6NFhUOTZkSVNaU0dNZythaXRsRUh4V1NHVFQ3dVJna0Q0bUlBSXdvQkZzVVdDbldlV01TUVd3b0NXM3VGeCtZalZHZjdjdEVjV1llblZZYlY2NWR4ZUxpRWw3NzlLZlI3WGFqL1ppT3BnNGx1cVZnTlJXWWlIQ09RU0JVZFlYRHd5UHM3ajdHenZZMnRyZTM4UCt4OW1ZL3RpVDNuZDhuSWpMejdMWGNXdTYrOU8zYmV6ZlpUVFlwY2lSUm9tYWtHUnN3YkZnd2JBOUdzSTFaTE1NampQMmYrTkhQZmpJZ0RHd01CSTFIR21rNEVrbVJvaVN5MmMxZTdsNjNidDNhcTg2K1pXYUVIeUl5TXpMUHFlNG1NUDFDc0pkYlZhY3lJMzdMOS92NTlzOTZKT09walVCTll2UmtRaklZd1dCSUVFOFpHa005VFFsV1Z6SmZVQzU4TWI1b2hrb3dxN09qbXNxRllZUlhBV1Vjdnd6UW9pdTJaMjFROHlucmgzdU1EdmFZeW9CNXU4MW9kWjN6MWM5NDJHZ1FTMG1Nb2JWNmljczNydlBTdlh0Y3YzR0Q3Y3Zicks3YVVKQnl1R3UyUFNraXo1dWRKdlZXamEwclczejEzWGNRS09KWlRLL1g1ZkJnbjZQREk2UnkvSWQ4T1NLOFBEN3liWWlwQ28xeUw0UXViTC9GZEtGd3RicERVQzVURCtLSmdEd2d2VFplQkowUUhta0s3eUNsa0lkWEp1V2w5OGkzOTFhM1BaUm5CNmFzQTg2bDIwSml1UTRTQXVHTEtwWTBHZG1RS0JPZUdGSHVaaGFHRzBhVUJvckN1K0dORnlhUzIzWkwxazdQVjExSllTNTc4d3NOdHFub1NrcHlYck00TE1IekVtVGhrZEtYWitKUGdiMSsyMTNOdmdoR2E0ZjJNb1lnclBIMk8yK1hMTktwUzdqSkVvNk10aFpnSVNReVVFZ3BTWFRDWURTazMrMXhmSHpLMGNFaG8zNlAyWGhNT2svUXN6bnowWkM0UHlRZURHRTY0VklZMEhUVzVBQ0lwS1FaQnRUclVaNlI3TFBmYmNXbGM2OUdpWmVYVzVRTkM2RVltV1hXYnlNOGpKWnI4MWxyUjd4NTZ6TGo2WnpoWk1wNE9tYytPQ01aOXBpL2tQU1JIRXptUkt1ckpHdHJUSjQ5NC9sUGZ3WlJpS3BIMURxcmJGKy96cDJYWHViYTlldHNibTJ4dXJiaXdqOVR1L2h6RDI0eDdMUFppUFZHalZwam02M0xXN2tCU1F2dHJVZjl6eU5GaUtCd0N3aXhHSFJkc3FycmlzUFI1TExsMGtIcFprSzZzanNyZ0tHVmdSMkxrK3BjdmJnb09QVG1aMlVWcEsrQ3pZZmZZakY5eUI5dDV3bEtna0lKdUxoWEVJVkRUSGdhZkxQRUhWVWRVR1RVa1Z6T3F5c3JFTE4wUGw4TUdCZlRVc3FpcDZLRk1NSXMrWGZLdzYyaU5QTUhoNlpnL1htNWdibUVXUlFFbHN5RVl3Y3lNay9DMGNZM3I3aGR1QmFlMWRUcStUTE5Qc2FnVTIySFhXZG43Qi9zYy9qaWtIZ3loaVNHMUpER0tmRjR4S3pYUlF6SEJIR01UQklpREEwRGliWUJIMXVocEMwVmtWSUV5aVVCWjh5NkJWTjIwYStLUENvOE8vS3lZWmYydWp5enNFZjFIMFIveHFQY0xDSTBLWmRiSWJwVEIxYlJCaVpKeW1TV01wak1PQnhPNkkxSHJFcEJjendrMlg5T29pUXpxWmpWRzB4WDF4ay9mc0t6di9rYlpCU2h3d0JWcTl0RDRlVzczTHgxZzh1WHI3Q3lzb0lLQTRRTWNqTlRkcTFaejFqMjBtdjN1UnZRdXBnVG1XS0NMNXpYM3l6ay9IbGJJQ0ZLdW4yaHk4K3E4SVJBSXZOYmlrV0hZMm14NVJHeHkvNkp4ZDJlOGRhTjViYmNsSlNoaGVKUkw4NElCSlFUQ01yWkhVRnA3bGF5THZzdmkzZmk1WHRTSHdmdFBUTkdsRUk5aktqSWtpb1AySEpkK3FMMHVGVE5ZQ3FDSkYrb1ZJRW5HbEdPY2ZZTUZubjBsVEJlMFdNZ2xkN2dTeFFuZVE0SzhjUTcrYzdadGcxWnVHZWFwb3lHSTg2NjV4enU3M1B3WXAvem94T1M2UXlaYXRKa0R0TTV5V0NBR0ExcEdvMENRcE95QWtRSWxJQWdVb1JTRVFZaDU5TUowMW5NV2xnajhvQ21CcE9YajdrRk9wdVg1N0puVWU1eDgwZTI4SjFMNFR2K1pPNC84RkZyVlQ2aGhidmFsMDNyTksvNkdsTFFhQ2pXNjAzVzJrMU91a05ldTdwSlEybG04eG1UK1p6NWZNNXNPaU9lRFVpUEpkTVV1a0t3cndVclc1Y1pQOXZseFljZkltczFVaWxwcnExeSsrVjczSHJwTHRkdVhHZHJhNXRhdllsVS9zSXpMVUpvVXhjdGwvMjhVcmpaZ3loZEJPV2pzaEJWYWVPdDFFeVZMVm14MmxaN2FTN2U0dm5pbjRLSVZINHZoQkdVVXN0RitXVVhIcHpWVUtaRkZTSytESlFzbGtxZXJSQ29sRzVaK2NaekFZWXBXNWhMTzByaGlVK0svLzlGa2dYckZEUWxCMk4xRVpQL09ObzdtUHc1b0ttMExQNlNVM282c0VvNWtSRnloUmVkaHhjUVdhcDh3QU9Ca3M4TUJFRnVsSnBPNXd5SEEzcTlBYjF1bCtPalE3ckhwK2haak5LYWVEb2xHWTZKejNzd25oRHBsTUJvbERFRU9pR1NodTFHblpxVUtCVVJPSTZBY1BGZDJXY3hRakF6NUs2L2NocE85aittcEhaVXd1RklUQUhlTEhwZVF6SE8wRzVLWGxDZ2pXUXBTZ3NQTDJhMGl4d1hwdXpBeWdheTJpQlNRMkJTMW11U3JVWUFNZ0twMEFibWlXYWF4RXhtYzZiam1PTlp3dlQ0aEkwNGhuMURERXlBcE5sRWIyendjR2VISjgwZlFoaEFHTkhlMkdUajhoV3VYci9PdFJ0WDJkcmVvdFZ1RVJXVFBRL2NVclovNTZ4SGJTcVhFQ1U3ZFdibXlsSGdlWWhNVlMxcWx1YndWYlRxcFVxNUdyMmVEL1pFNVIycUdPK01xS3kvbDZnUERZdkZiekhvOTZHZ3hzc01YeGk0a1JOaXlqK2tQMHhiL3BxYmtyNjRyRTIwSmJXN3B5dFpCOXJQQ2ZEdGtDVmZoU2R2clZZT2xYV01UMllwdWRaTWlVQkFLZjFkK0tZajZUSUNNNXFzUkppVWh4OSt4SU5uenprL095T1p6WkZhUTVJZ1p6Tm12UjdCYUVURENCU0dCcUNNSVJLU1VGcHN1RklCNHpSbE1wK3kxbWdTYWUyQkNOeElTaFN5MkVCS1p4a3VCRks1dzA5NG4xMCtpeEU1VEJRUGNtTFhpMDRwbUllQ1NwSWtvUUFRRnRkUC90Skx2L3pOREZmYXlaaUx5RFQvZERhdTlKWVlsUFJrM2RwdUQ0SkEwQXhDVEMyQUZjRm1JamsrN2ZMRzFRMTBQR1U0bVRLWnhVeW5mZGdmb1ErZU14T0NrWUZ1MU9Ca2JaMlR0VFh1MStza0dJeFNyRzl2Yy92bUxRYVBIbkJOMk05YVpqQU1Uejd0MjZOemI3NG81aWJaR2hWTkdkcHB5c3hzVXlwSHE1b1RqOFVyaXJDWElySFpuNmVKSE5aWnZqNU5pZTVRaWordGVqLzhpOUNyVDBxekNlOXRDVEt5cVhWa2VmMjMveUFLRjR1ZzB3SUZsVDludW5EWUNWVWdyb1FqUU9iQ0RVOUY1dzh2QkY0R255NitSV204WERtM1JWQUZhZGhRMkQ0TGxhRGZqbVFqVmxNYXBJaEtJcTd2T0ROdVo2YTFId1ZkRUZReTU2bVVna2dicHIvNGlMNVFDQlZpSmpOTW1oQUJUV2xvSmdudE1HQTFzaTk2b0JRS1VHNTQ2RHBVNXFrYlFLYTZpTE1VM3NNbkN0NlBrc0t0K1V4cGRadnYrTjJhU2J0Y0I5K01ubW56dGZ1OXBuSENjRGptL095TW82TURuang4U0VTS3hIRHJwVHRjdm5hZHJlMHRWanN0QUZLbDBJUldOMkMwK3ozSURNbm5yQnF1cjlhNk9FcmQzQ2lRVnZPUE1DUXVVVW1hSXFqRkdNYzdUQldSTUd6V0pJMW1EYm5ld0VoQnFoUkpraktiSnd5bk04YXpsTDNCa09IQmlHYi9sTjRzWWFvTnMxb044K3dwK21jLzRXNmdDV3ZLR3RLRThVUXp0a1NTbVhmYkxGR2tDVlBDb1N0dmU1QmowVXdsK3R0UVJuWm5hMTFqeW9JdXNpMkN5QTlKUVhYZDdzTStmWVZyMmU1dFREa0x3bFNhRkVGNUNKL3pYMXlCRlBockE5OTNuQkZXZkd1dmxESW5qaG9oQ3l5VktFSTF5QkpraFBES2U3ZDFsVjY3SWZOTVZTZDJXZlEyNVZOTlYwbG9aL3ZNQm5QS1dWZTFOeGJOQ1ViQ0lMV3NsRmJDZTZsRkVRaEN4Y1VsZEw3cXl6L1lURzh2N0lzWXBvYmJ0VHE5eVl6NWJJdzJtakFLcUFjaGpWRFJIZlJZYTlaWUNRSnZwZVJMcDYxQlJXYXJUeUZJZFZaZWF4ZndJSEw3c1JUV2pLdWNlS01JanZRTVNPNWgwMEl3RjRLcGtJeFZTS29DWmdJbVFuQXVCSi8rOElmODZJTVB1SEwxS2xjdVgyWjdjNVAxVG9meG9NdHYvSVBmWXYvd2lGLzg0aU9lL3B0SGpIcDlndUdBYWREa2tneHBrZEl3bXRnSTVvRmlIZ2FrSW5aQ0dudVFaVG9LZTV3SHlFQWdaYkdhZE50VSsvMm4zdDFtaXRXbEVNYTJRVklnbENTVUJsRVBXS2tIYksyMWJhZS9kOFpnT3VOeXA4NG9uTkdmemhuTkoxeUs0VnFqeFVxa0NLVDFET1J4NVY2Y2ZWa29KOEdrWG5WYkRPQ2tTOTdKQjM1WkZTTUtZcEQwSzFtZnVaQ1JzWTFUY25vdnY2Q3N3WkdlWmJoa1ZQTG5uU0pyL3pMQWRyVjlGeFVURjI0bzZ1Y2pGejk4NEdlS0N3RmF5anpWSkVkelNXOFltTFBGYy9adTNqYVlDdWJKQWt0bEViV1V2MXlVQ2IvYTVPakdiSWlZNTY4WVg4aVE1bmJTL0pkVFZVb0phWVVxbFBzeG43YWl2VDFzdmhHbzdITkxGbUJuZ1pVWmhzTlk5WmtRRURWcWVWK2w4NWJGQlh4azlFUXZmQlNkQlVoSzUvYWpFQjZKd24rZnJUbDFKb2JLQVMwUU9oSnNiTGZrYUtHSWdWZ0s1a0l5eFhCc0RMTldrMTZyeGNyMk50dTNiblAzOGpidEZSczczbWkzQ0pSRXB5bHBxams1UHNTWW1MdXYzdU9sMSs1aGtHaXRtVTlqQnQwaFp5Y25IT3crWisveFE4NTJuaEFQZWd6dkNCNUVEVFpGUWdkTm1NU0VJa1dsQ1VGbVgxWldHcXpjVEVNNEg0YkVvRk52ckdhODNFTVoyTUdlc3I4RGpTNnk3NHpMT1RRQ2RFcGRoVnhwMXhHdDBNNFUwcFJRS3BRRUtYUVI4cG5kekxwUW5XYWJBdU9IcGZvNE11RUZ4aHJmbTVMbFRXUStpL0pXSUlPYytzTkJJM1QrRUdZSGQ1QkJQMG84RGw5K1h4WUVGQnFYTEJ1dWtpMm1QY2lyOFJGdExtekc2UUJ6WEo0MitmZmdwNzlqaENxWTlWSXN1QTc4ZmJyMlQ2NXNVNW9QRmpVTDFJV1NHVWt1bVpDNkY4Wk4vbXptR2Juc3RKRHhlajUwc2tyRU8rMmQxVlJXQmpLNUlDa3JyR1VGSTRUT1ZZSWxUVnpGemlpRTFhSGJpdGlnSlhtNnNzYStuTEdEY2NwOGtDY3FrZ2FycGMrMktwSWlybHhuVW1kaCsyY2p3Q2hGSWlTeklDUTFtcUV4REl4bWJEUXpJVkhybDFpN2VvMHJ0Mi93MXRZV2E5dmJySzZ0MFdnMFNGTk5FczlkaStiT1hhMXo0a3p4OEVxa3NwNEdLUldxVTZQZWJMRjk0eXB2dlBldU5TUWxLY1BCZ0xPVFl3NWZ2T0JnWjRlSGp4N1EzOXRCOW51c2lKU1ZRTkFSaGs0b01TSWtEVVBTd0lGRDhud0lTa1Ruck1JS3BUVTRJVkwzdmRrRko0N2dxMU5CbXFhdWxjcGlFQktrTVNncGtETEZLT1U2bENMRElDdlhNd2w3ZGlObmgzVnVjczVNWFZLVW5JK2xqWk8va3hLTG5wVEZOYmZNTXpDRXo1TEtTRkhHMitySUNudk8xd1lZVTdIRmUyeEl0SHNaWlk0djkwc1JuNElsaE1Fb0J3WE5TbzVTNXB2STlzdmt3eWlkdTU0OFpKNHpNVkJhcllpeXFBZC8yT2NGZkhvYmc2ejBrNzY5V2hSOE4wK0VWWGdKaE1ESUREN2hiUTZ5ZUsxbGpramhlOTZOSnoybW1CQ2JySGV6RmFvV1dXcHZrbU8rTUphbUk0QkVaa01qTytzd3h0Z1NXVU9hYUJUV0NpeWRqODU0cDRvUzB0MVd1QlFZU1NJZzFoYWZsV2dOZ1dSdUpLZWs5T3NScHpxaGNlVXFHOWV2Y2ZQYU5UYTJ0bGhiVzZYWmFoUFVhbWhTWjkrMWovdHNPc2xmdXV3bTFLWlF2R2wvT21yTVFnQ0hWTzUzVEdybk1RcFdWdHQwMWpyY3V2Y3lnbDlIYU0xbzJPZjg5SXlqRnk4NGZQNmNnNTFuZlBiOEdjblpNZVB0R3p3Z1lCTkRtS2JVRERTTUlUVENEZ2V6ZVljS2lKUkFCZElkcGRwRmFFdFBnWm1pVS9LZjBlOXpwUkFnMVFMTkoxOTdtc1RPU1hTUkFValZ4Wm9sRE9sc2pXWnlnWkh4MHBweWQ2eXBSTU81ZDZUNmw5TENHOGFhc2diQU03dnBQQm1wK3YxbEZrdkhmeXhSVDB4K2tBdFBWcStrc001VDZkeVY3c0dlajhjTWUrY1pFbXpSWFN2RW9vQkI1SWVBdjI0MGxUV1JLTzFURjUxNW9zSnQ5VzJzQlprbnU5bWxLVTdaVEVKYmJQbk5raHFpTE5qSURwNnkwRkc0WFBWaXlGbUVoSHJpQnZmemFrOUhYdkp6Q2RmYlpoV3BNYVZaU2habUtmT0JvdHUwdUJ0TkNHa0ZLVUZBSEFUTXBXS1NhcVlZSmtJdzBwcTAyV2I5emkyMmI5N2lhOWV1c0w1eGlVYW5UUmhHaEZLUXVtQVI2Ujc0eENRbCt5ZXBKaEdKUlpHSnJOMlIzbkpaRjYyS203Ull6WHRRZHRHNUtzWnF6RVcrb3NTYlhEZmJUUnJ0RnRkdjN3UytoVEdTK1h6R2VEamk1T2lJbytmUE9YeTJ3L0dUaDR5ZVBxSVJ6MWlWZ3JiUXRKV2hGUVoyZHFBQ0VxRUloUHZzc2dCVnA3ck1Xek1CUXVzOHBkaUlJdExiZUFlODhRZDRXbHVPZ2xjOTVyeUowZ0JPNDVXZW50N0ZNUUV5L1lYMzhtVHpSR21LRk9sbFRzQ3NOczJUbjcwRFdMc2JONCtlejJ0bDQ4MjVLdExtN00vWDJtNDd0Y1c5SzJBK21UQWM5QmljblhCK2VNQ3p4dzk1L3VReFQrOS9TbmQvbDhEMjBMTGt4Qk1zQ2ZBUTNvb3BVNVY1aTB1VDlUbW1ZcjZ0NUwrSlhEY2d2WlBaT1pYOHN0NFRReGpQakZPeG5aZVdMYklTQkdJOFlaOS9RUGxHY2VGbHNGc210eW04N2tZVWNWUEdTa3g4Y0luSjVnbDVlcXo5SE5LOFhSSDJ2NUhDUlRRWEtQSllCTXlBOHhENlljaXhFT2gyaTgwN3Q3bDg0d2JybTV0YzJyeEVaMjBGRllZdU1kaCt2eGtvVk9zRVVoOGZybk5xamtEa0hQNVVhdzkybXVZQkxVTGEyM0k2bWRMdDlUay82M0orY3M3YXhpWXlsR1ZDa0hESnM2NFgxajQ0dy8zZU0wQm43ck5BVW12VXFUVWFyRzl2OE1yYmI0QXhKTE1KMCtHUTQ2TVQ5bloyT1hxeHl5ZVBIakY4OW9SbVBHZXdkb25WRk5aVVNGTW9JcUZSOFJ5WmFsdEZpU0lKM0xaMHlxNGtzMFRmbk5TVUhWQTIxMHNuQ1NpVCt3TVdZc05Oa2Uxd2djeTEyQ1I0VGI4L3g4QXNwd0daQ3J5a0ZCeFQwZjluTDc0U0Rxa21RRW8vODgva0xYcVd2cHdtTWFOZWwySDNqTVA5Rit6dFBPSHB3d2ZzUHZ5TWsrYzdtTmtFSmJFY2pEUkZHczFxcDBrZ3RIRTlyK3RmUFJTeThMZUJCWnFmWW50U1pKRkpRemtyM2xjb1pha2xNZ05jdXNkRFo4R1d4YXpCNEtrSHMwbDM3cE11QmlGWmlHTTFsTlQ0VXVCUzlKSXBSQng1eVNTTDRZNzBLZkl5ZHpwbXJZREc2dmhUWTllRjBrRXhmS2FpeGhKck5KSkVDR1pTTUZDS1dNTFV3RXhKWWdSSkZLRTJON2p5MHN1OGN1MEtuVXRyckRrT2dGQ0NWQ2V1UlhhSThEU0JOUEdZeXBuNlQrZmJVcDltWTB2REZKTFVlY3dWZ1ZBSVlEaWMwTzMxT1QwOTUramdrQ2RQSG5ONGNNQjROQ1FBZnZEOUgzTDUyalhlZWU5ZDNuenpEVzdldXNIbTVnWmJtMXZVNnpXa1VxU1poOFBvRWhYWG1DeEczT1I5YUpZSDZUdjBnbHBFcDdaQmUrTVNMNzN4ZWw2U3o2Y3p1cWRkanZZUE9ONTd6b3RuT3h4OTlqR21lOHhLS2xuQjJHcEJDaUpwaUZWc241QXdjSXBIUVdhb2xXNDdsUjNnYVY2RnlYd2JKTHpCa0JVelVhUkFaZUVzWG9TWEVxSnNNYzU2ZUNvNUJ4VGMvZXlaS3l0MHMwTkFlM1o3VTBrUWdsU25hTGVaa0VJVHVnM1JiRGhnMUQzbDlQQ0kvZDJuUEhud0dVOCsrNVR6dzEyUzhjRE9Rb1I5MlpXQWpWckk5dFZOcmwrOXpPWnFoMmF6UnFkWnB4NEZCTDRDUnhpVHIrUUs2M2VoR0N1ZmVOSVJmNG9BeEZ6cFpIU1pNcFQxNVZwNFFSWEZWREtETWtoVEVWRGthaXZoV1c4TEdaRDAvSkRad1NGeTZhYlRGQWh2U09KaG9YSzZrQ3VkWlo3OTVnUXZTaGJwTnRwaVFlSlVFMmViSy9jU3BOaXRReUlrTTJCaVlKQnF6cU9RWXdIMVZwdVZLNWU1Zk9jbUcxZXZzTFc5eWFXTlRWcnRGWXkwWHpPTzV6bEwzeGlOTnRKcUhrelpvcG50YnpON3M4eHFEc3V4c0JjQUFDQUFTVVJCVk9GVlYrNmhqK2NKM1Y2UHMyNlhrNk1URGw4YzhlRCtBODVPajBtMXRoU2hMSlhXWUJOcmxXSThuZkh3L24wKytjVkhUaGdFU2lrNnE2dTg5L1d2ODdXdmY0M2JkKzV3NWVwbHRpOXZzN0t5UWxDTDNNWkhPN2FkeWxIbTFtNWZwRXE3c1dpQnNNNVdyTkxRYUsvUWJGL2k2dTJYM1RPWGtzd25kTTlPT0Q4KzRXaHZuMmVQSHZEWi9jK1k3dStodzVUMXBtUTNhTkF5SVEyTVZWaVNXcGVtQnBQb0hMaEttdVlTY09Wa2tVTDR1WTd1KzVFVmRJN3cwVkVVeWNHbTNKc1hRa2p0QlhXNDUxaVlnaHJrVWE5OVZ5VWlKY3gwcW1uQ2JEamcvUFNVczZORERwN3Y4UHp4WXo3NThHY2M3KzdBZkdMRmFSS0VTWW1Vb05tcWMybTd6Wlh0VGJZMzF0aFk3ZEJwMW1nMTZvUksydW92VGIzNVEwSlE3VmF5NUN0ODZFRUpaNnp4VFdWaWljRFl0eDRVQWZFRmRrc3M4SHBTcjNRMFJZZXZSY2w0a1JGcGhhbXNQckw5ZzFtbXVQWlArZUpBazluSXo3SDZNakJFQnVHUVVtYVZiVDY1am8xZ2JBeTkxQjRDc1RRa1FqQ1l4NHdEUlcxcm04MGJ0N2g3K3haZjIxaW50YlpLczlFaXFrVUlpUlhBT0RIUlpEcGE0TkZseWtrcFBkMmt0Z214T3FNYkNEZWRseFl5T285bmpFY1R6cy9PT0Q0KzV2RGdpTDNkNSt6dTdEQ2R6ZXdhVEtwOGMyUmtEU05ocm0ybGt2V3RwQWtCaG1ackJXVU1VcWZVakxIekRaT1NtSlNmL1BqSC9QVVBmMEFheDVaNGhPSGwxOS9rdlcrOHo4dDM3M0xyOWkydVhiL094dVltaldiRFdueU5MdEF4K1VTLzZydkxmcjlwaWVtUE1BUzFrRXRYdDltNHNzMjl0OS9rMithNzZEUmwxQjl3ZG5MRzhjRVJ6eDgvWXVmcFl5Wjd6d2hIUFZha29HNE1IU1FOQTRGVXpJVWkxYmFDUTJLWmpDcEFwTEc3Z0dTK0dwYkdlOWI5cE03Y3M3dVkvMURTcHhzSGgzSFB0bTA1TkZKWjdZcVFnbEFLSWdGcE1tTSttWEIyZk1ENXlSR0h1N3M4Zi9xWWp6LzRLZnRQN2tNOGQ5dkpGSjBtQk1LdzBlNndlV1dMN2MwMXRqY3VjV21sU2FkWm8xWUxxWVVLWVZJbjdYWXFHVDBsTVVVNnQ5ZEkyMlFnSVlyc2VGTWxuUzZCR2VXZ0JiR2NSR3FFV0FJMnJMZ2FTam1CMXJYbEc1S0VkM0xpOGM3Sys4U3FaTkozRWNpSy9OaDREa0s3aXhmU3ltSXpkSldSeGlLdFZFQ2dBa0lsbUt1QW5ncVlHVGdOUWdZNnBSY0dyTDkwajZ1MzduRGx4alhXTHEzUldWMGhDa05rVUVNYlRackdkbjl0QktrYnJHVlFVRk9GTGhoOHZhNnpHMmdQT1NiUlNjSjRNcVBiUGVmMHRNdkIvaEdIaHdmc1BubENyM3VldzBXek5XU2lJVEVCMmtqU0ZLUUo4aXJKZUR2bGZKSXRKRnBvMG96OUwxMGV0c3BtbHpvL29BT3RNYW5HNklTZG5WMmVQbnFFd3RpZldXdWFuWFcrK28xdjhQTExkN2wzNzJWdTNibkJsU3VYV1ZsZG85Nm81K0lZLzZJcFptbHBvWmt3M2dvdkh5c0lsQXJwYkd6UXViVEo3VmRmNTV2Zi9mdVFhc2JqSVlQemMwNE85emw2dnNQZW80ZWNQUGlNOU9TRS9xVnRIaEN3clNLRzlTYkplSlFQRFBOUTBweE5rUmJrcFRKVUVLVDBBazl5MDdCbjFuVkRaZU1HY1c1ZG5Nem5UUHREK3VmbkhCL3VjN0R6bEtQbk96eDllSitISDMrQVNPWTBJOHVIdEd6NGhLMVdneXUzcnJPOXVjYkcraXFYVmx1MEczV2lNRURaNlpKVFhXYk9EM3ZER3grMW43MEh1Y2ZGSnlaSkFweHlUb2pQZzNNdVUvdDcrQWFQQmxURytsZnBJcUk4YURGVWNjS2xPc0szVDVaaFFib01RVmlvUFR6Q3V6M1NjNGFkM2JYclhMS3N0WGJ3U2NGc05xZmY3YlB6NkNtSCs0ZnNUU2JNTjlZSnJsM244bzJiM0x1OHpmYmxiZHFkRmtFVUlZTXdyMHF5K1VPaUUvc1lTT2xhRkZQWWhrV0dXRk5PcCs5bklraTMrelpNWjFPR3d3SGQ4ejdISnljOGZmS0VGM3Y3SE8wZk1KL05YVWx0UDQ5RVEwcUFTWVR6d1dVY1BwbEhZb0d4bkFBaDh2QU1tY20vTTZPTXlHakZFTWVabkZ2bXU1clVnMndLRWJrRmNnU1JSaHBqOS9LcDNjV1A0NVMvL0l1LzRQdmYrNTdWMFNjem9uckV2ZGZmNHRYWFgrSE50OTdrOXAzYlhMOTVrODNORFpyTlZsRXFaM2JlMHNFdHl2OGM0MVNwMlZRcXdTaEJvOU9pMFc1eCtlWjEzbnIvUGJTZUUwK245TSs2SEIwZXNiZnpuQmRQbjNMNDhBR2oxakZQOTNZaGtEeC84Z1Fwb05OcElrTzdma3lTaE1RWWhMSnFMSm0xdlZMa0xZeVZqMnZISU5Rb1laQkdFMCtHekFZRHpvNE9lZkg4S1kvdmY4Yk8vZnZzUHZ5VXdlbVJheTl0bTJwTVFpTlEzTHArbFp0WHQ3bTAxdUxTNmlxcnJUcTFla0FVQmtpVFlremlEc1FrR3kyWExoQ2ZZaWg4RjBEK0dmbEpSWVh0Vi96WnMrSG5rTXN2WWd5WHlmRVZVbG1GVGlJUUZVeHFMalZlQUk4dkhqQW16N0NyYmlvTllxa05TZVFaQUFXbTNyMmtTVXlJSnAzUEdIWlBPWDd5R1gvNGYvNGZ2UDc2cXd3SFErWnh3dGJXTmxldVhHRjFmWjNXV29kV28yYVpmVWJrMlhPRmMxQjZlOTJLbm1qSlFNanVubE9rbFBrdFA1dE1HZlFIbkozMTJYL3hnc2RQSG5OOGRNUm9NSENma2NwWGthbTJtNGhZMjRjeEpZdGV3N005TytWZzd2Y3ZEa3dwN0FPc2dzREJSQVZTV1pEbWZESkVtb1EzM25vZG5ZTFdxWDBKa3BRNFRvbmpPWWxqSWVUbTIxTGdqVmYxbVd3LzRyWWpKc1hvQkpQRXRnL0hvSFdDVklydGE5ZDU1ZDRydlBmdWUxeTVjWjBiTjIrd3Nibk95bXFIVU1raVQwOEtSNVR5MDVORmVZZnYwNlpNV3JTU1JoUUp5VWFTSkxhRk9EazU1dVJvbi8zZFhSNDl1RThjejdseCt4YnRab09mL2Z3WGZPZS8reGUwdG05Z1pHZ1ZDYnJBd0NrTTgrbUVmdmVNL3RrWlJ5K2VzLy9zS1UvdWY4ckJ6aE9HSjRkSUhSTUdkbnNDR2lVRm5YYUx5NWUzMkZ4ZllhWFRZVGFiRU1tVXV6ZXZJVVhxMmxOWmNCcnlwcW1FOVNnbGNPTURhaXVLdTV4bllVeU9FaXU5TFgrMk96U2xVRWF6REYzc3Y2REcyNG42a2xyS1VTZWxQMGd1K1h0TFRveEt5R0doclBxY3lzU0lMN0FlVzhubnJIZktELzc0LytYRm93ZWN2SGhHWUJLdVh0bmkydFZyWEwxNm1iVkxsNmpYbTZoQW9keURsaGdMelV3VHF6b1RaTnpBWXFHWURaR2tVbVZTc2JHcUxPRTh0ZFBwaUY1L1FQZnNuS1BqSTU0ODNlSEpveDBHM1hQU0pBRnBCM0paQ2ErTnRHNDlHYnEyVXpwOXZmSDJ4NkswbXNtbHNpNlJKMUFLcFd4bElhVXE0clc4eVpOMkUvckplSWhTQ1crKy9scVo5S1NMejFVYlF4Sm5CMFBDUEVtSWs5U0ppbVJwYUpieEVxUURiOWhOVHFicTFPZzBKcDdQRVdtS01JbVZKTWN4R0UydDFlYnF0V3U4LzgxdjhNb3I5N2p6MGkydVhydHEvZitOT2lxSWl0STJIeElWU1UyNVlNaVQ3bWFEUEdOa1FZUE9CcitrcFBHRWZ2K00wNk1URGw2ODRQbnVjNTd1UEdlV0N0YXUzbVRqK2kwYTdWVU9EdzU0OGV3WkR6NytpRWVmL29MQjZUSEpkT3k0aTRaQUdtcFJ5R3FueGJYdERhNWUyV0p6ZlkzVlRvdDJ2VVlZS2Z0OUpna0hSNmVreVp3YlZ6ZWRGa1Y2b1RqR3crMFh6MTRtdzlkZUVsZUp5MkZFSlYvSWE2T2txS3pwQlVHZVpTNjA1Nk92NG9nOHJ6d0ZIVmFZSmVRT0R5NVlLSUlMWGJ6SWpUKzZrdFloS2d4K1hRNVE5UDNaRnhxdXEwV0x5ZnZKd2RFKzMvdkQvNHZmK3lmL21QVmYveHIxZXMxeDZOeHRLWXZWUzJxS1lzclBheThZQWxhZkxseS9KNld5UDRLVTZGUXpuYzdwZDd1Y25KNnd0M2ZBNHdjUGVMYnpoUGs4ZGJ0cSs2Z21HakFLTFJXcHl3K3dCNEgvRW9seUgyd3k1NlY3Qll5OTBhVzBVV0hXUUtOc2tnM0ZEWkJKWHMzaTFOR3RtWW9vYWJRdUpTVG4wRkFNVWFTSVFna2l5dmtEV2h2aUpMV0hRNW95bnllazJobVVURUdwU1RKUnJGQ0lRQktHTlZRMjljN2dJMm1DVGxMMjl2YlorOE4valU0VGtqUkJDc25HMWladnYvc3VYMzMzUFY1NitSNVhyMTNueXRYTDFCc05hclVJaENIVnFiZCtUSXRvczR5R0xJb0QycVFwUWdZZ0ZFWUVCTFUyalU1S2EyM095amloZVRMZ3IvN2tUM2oyN1A5MnVYclNydGUwTGNmRFVISnJzODNHK25VdWIxeGlZN1ZGcDFXaldZK29SeUdSdEZWV2JqUVhxUjNtSVhpeGY0QlNpaHRYdDIyMVVvQUFQTkF1WG5DdEFSRjRWMm81blVtSUtsL2ZjeGxtQTNCTkh0YVJ2VkdCRUxtZnJoQ3FHRCthV0pkZWJMR29peWlsOEZhRE9lemxsWG9HUlYxK2dUMlBkTDUyTlJYSWdKOFZXQ1VMK1ZOa2ozZG0vU0orM0xnZ0NrTXVYNzVNV0srNW95RlhoV09jVzB1S3dsdFFKdjVRTWsyWkpHVTZtOUh2OVRnNzYzSjBlTXlMZzBNZVBielA2ZkVKZ1F5UVFaQ0Q0MUlEaWJZOCs5U1JkZk5mby9CS2FsTko2bkdDakFDQmtzcXlCSlZDWlpzSzRVMC9LdFFIa2FPN2pTZnZyakFTalNmOTlseG93Z09ORkpnMVVkcnE1RndoWmFzTmFxYVF3VHFFV1pKcWtqaG1uc3hJWW9OT1U4OXE3WkorTW95YWtLQVVRaG1DcU9hR1hJWkkyNm4yYURMbFI5Ly9BWC8xSC8rQ0pKa1R6K2ZJTU9EbTNWZDQ3MnZ2Yyt2MkRlNitmSWNiTjY2eHZiMUp1OTBpQ0tQYzd5R0VSR3NZajhiMCszMzI5blk1UGo3andmMUhQSG44aUI5OC8vczgvdVFUR3EwR1VSVGt6S1JtUGVMYWxTMnVibTJ3dGJuT1pEUmlZMzJWMjNkdUVLa0FTWXBKRTZ2WDBLN2QxQ2xaZ0dGV3FkbTVydUxwemg3dFpwM0xXNWVzbU11ZmlrdFJkdlo1UTdYU0hXbXErTXRLRytvSmpvUWYwR0Zrcm9ZRjQ3d0FudGZRTEZuU2lHckFtQ2pFSGFYaU8rUEZDMUdlYkZjV2Y2VkFVTXJDb1Z4OVZrMWFFVXR1cnVvc3dwZ0Z6SmdwU1RSRXZ2cnpxWTdaMXc4QzVmejNJaTlmcFJUTTV3bmowWmp6N2htSFIwZnM3ajVqNThrejlwNC9aenFadU9HZFFnczdmZGRoaTlTVjhjYjNWZ1JGbTVTNncxSnFQNkxNOXBkaEVDQ1ZSQ25yWlJjT1NlMVRpa3hKVjhheXBFa25ENzRvSGFsc1B4TWxzOUx5TkF0VGtXeVhhTkRlelNxYzdUWVFoakFNTUhXRm9PWXNzWUkwU1VpMUpvNWpadk9ZeExVUnBmbENWcGtwQXpKd1AzczlyMVFDTkhXZFFxbzVQanpnai8vTi8yTmJLYTNSU1V4bmJaMjN2dnBWN3QyN3g5dGZlUWVFNVBIakp6eCsvSVJQUC9xWVI1OStpRkNLV2hCZ1VvM1dDU3BRZk9XTnUyeHZickM5c2M3NmFwTk9JNkxkYnRKcVJDZ1Y4UFRaYzlwQmsxZGV1b3JSYzBUaURGWWFXM0VZVHg2c3JiclZ6bGh0blB1akowOVpYNy9FMXZvcW1CaWZnaWtxU3NEcXUyZU1iM3ZYVldoN1Bnc29YWHlsUXM4Z0hYazZvLzhFaTZXN1A5bkpVRk0rR1VibmYwOElueDdza1VhMEg4bnRqZXp5dnFhNi85YjVkV1E4U0VJSmJuaHhuZTk2VlZWZ3FUd1lxUDIrN0pvbk5TbUpUaEdwTGp5ZWpoMkFrS1J4ekhBOHNTLzc2Um1uWitjOGVmS0V3OE5EenMvT1NkTTBkeG1tV3BCb1F5cnJEcFlDV3JyYlBQc1pSVkd5Ri8yVlhic3BCSUZVcU5BeUJKV0xDQk9lcWNxVWNGQmtWd2hJNmZGU1RDVXByVkJ4bW9WcWFkbk90cXdpTENTeG9yejNOa3NlUm9vQW1NWFJqaWhjb3g0WFd3cUpEQldCVWRTaWtFNjdNUFVrYVlwT05mRThKazRTMHRTS2VISm1oNUc1cDE4SWhSWUtHWUFLSWxTa2Jic3dIYU9GWWphZThOTWYvaFUvL2VFUCtkY09US05OZ2pDV00vREtyYXRjMmQ1aTg5SUthNTAySzYwNmpVWWRKYTMvUU92VVZUMFdyeklaamZuczRSTldWenE4OHRKTmpIdnhjL0ZiaWFmb2h3MFowdFN1Vlo4K2ZzTGxLOXVzcmJUdHFsR2JNdlpiWktFMFBuYzlxeEpsQ1paYi9LNG9DZVJLN1c4R1NmV1FZS2tEdDJnTnMzbEtrQUV3eWp3alhmNmx5dVdacG1VTTRhSUgzMUFocmw2UWpTNFFYaFQ1c211cWpDZ1RDNVhLa3FHaSs4VklaekVVQWhzMmFheDhOazRTaHFNUmczNmZrNk5qZG5kZjhQanhJMDVQVDRubmN3c1ZrWXJFbGJMYWxmR1d2R01IZm1tKzk1VWxJSXpVM29udWJ1NVFCVWdWSUpRa3lNcDM3M09VbnY4Z3J3ZHlhWmtzeldLTUxxeld4UUIzU1kxVmdydGVmSWhhTm9ITUN2SmNGVmxCU0JRb01qemN1MjhFOCtnNHh2ZzFxeWhXemJuQnk1UXdWUWlKaW13cjBHbzF5Rnl2SnRXa2FVS2FwTVJ4VER5ZE01M05pT09ZSkludGk1cGtRTkkwcHlNYk5GRWdhYmViWE43ZTR1cjJCaXVkQnEyR1ZjYlZRNG5Na3BFeUNKQW9qRlE2MzlvSXVzTXBIMzM4Z0d0WHIvTFM3U3Uydk04dE9yb2tPemZleTYvZGlqeE80TUhqaDl5N2M1Tk91KzRxaFN6SHp4MzZ6c0ZtUkpyUHo3TDRNaDlsbjAxbXBWRGxmQWNNcVE3Y3hXY0FPOStLMDVqUlpNcGttakFjVHVqMWg1eWNuWE4rM21jNm1XVlM0RW9wNFRVYlJXNTUyUkF2cXNNL0Q3TmxVMmwwMmNoemdXakliMXlFWVVrK0FibHpTaGlSeTM0WHYrZVVzcVhQaFY2NHJEK2w3QWZ5WjMvNjV4d2ZuYkN6ODVUSlpGcjhJbzNkcnRwRTN4QWpwRDB0L2VUZ25EdnZLQ3ZHKzNtZHdpOFF5dGt2TFl0T3VKYzlqOVUyUlpzblRFVms2ZUdWVFVhUzhZWTZaVCtsTEw5NHhvT1hMdCs1TERrc2hiOUNLR0NaV0FpTU1HV3NRMTdMNVQyNzhEUWdvbEpzRkw4RFdRR3lHbE1KZThvL0RPdkp5QTRQQ2FUeG5NbGd3SFEwWWo2ZFlMUWhUUk9NVG5Pd1NEMk1XRmxwc3IyNXp0YjJCaHNySGRaYWRScjFrS2dXV2ZTWVNVblNtU3MzWWx2Mis1SmtJYXpQT1p1V0N4Q0I1T1IweE04KytvUlhYNzdOalNzYkNKMlNBZGlGa21XZ3JxaEltd3hNNXdrUEh1OXc3KzRkR28zUVZRME9WSUpaM0hCNU1UODVjdDVMa2NxS3l0U3hGaDJMbXRrOFlUYWIwaDlPNlBiN25QZUduSjZkMHg4T21TY3BRUkJSaTJwRVVVaW9BdGJXMWxGckVPQ2R6Q1ZSYzdhTDlJZEUvakhuKy8xOXJyN09EQml5ZEdnWVkwckVsRHdFTkw4cFhBK2NkUzVhbEc4VjQzT1JwSGZnWExBaXpOSmdITFpzNDlwMWZ1dS8rU2Y4K00vL2xBY2YvY0wyNmc1amhjZ2gybGFmNzRxZTFHTVdXZ1dwY1NzMmdaU2hYYkVwUmFBQ1p3MldoUTNYVklKUXpTS3JyVnpXK2NUWUtoNHRpN2lTQzk2eUhEOVZ4VXZqbzluRnhiZC9qbDgwNWQ3VCtQTVlVWVNLK0twTVU5bEpHei9HVFM2ZElmaXBUMFdLclJmYWtzM0N0S0YzZWtMM1lCL2M3ZDVxTmxoZGI3RysxbUhqMGhwcnEyMVcyMDBhdFloR0VKQzlqOUpEd21kRE5zdXpsRVUrb1ZHWmU4dzVPcDBISlB2K1pjVGhTWmVQUHZtVWQ3L3lKcGM2TlFRcEdHV3pHd0dkWnNBU3J6TEtXSlZTTVp2RVBIcTB5NnN2MzZGWkQrM3ZJbld3RStmQ2xkN1EyM2hUZisyY2pVb290TEZtcG5pZU1Kbk82RSttRE1jenV0MEJ4NmVubko2ZE01dk9MVXVoVmllcTFhZ0hBV0VVc1hscEN5RU5vUkkwYWpXYXpTYXJLMjFhelJydFZnUHhaODk2NVlnZjRRdHRaS0YwYzNvQWUrcm9oYW1pdjg0VG5vaWhxQTZNbDkyV1FRK0V4K013WGc1N2RodTVsVVdPNXBLNTZxNTRNRTJsbVBEK3Y3ZW1pQUtGU09lOCtPVG4vTy8vNHo5R0tvdkExR2lVRTVab1VRaDNKTUxDT3BSMEwzcGc2Ykl5eTQyakVxSGx5WjByOGVVK0hGT0w2a2p5UzZkQWx3NEZrVW1LL1FqclRQZ2pCQmQzL3FJa1hNMlU0ZlBwR0NVU1huM3RsWklCU1pSdUl1YzFsOUpyQVp6b1JzaUYyWTUvcStXL2E2KzBOQ1d4aXZmMzA1aVRGM3Nrd3lHdjNiM0I2M2R2MG03WGFkUnJCTXFhWDR6YkRPUXgzdm5HU1ZSQ082WFhEM3V1VVk5N244MkpMUEJWSWxUSTg0TlRIajdhNFd2dnZVTzdvU0JOdkxhb3NBNExDdUNvallxM1gzczhqWG44NUJtdjNIdUpXbURLY1Y0NWRFamw3YSsxZTB0bXFXWVN6eG1ONXd3R0U4NjdYYzU2UFk1UHV3ejdRNHVocTlXSXdvZ2dDQWhEcS9VSUhBd2xqRUlhdFRxdFZvMU91MEdyWHFmVnJCRkZBYUZMb3hLZXR5RVFTNEFnb2tMZUVkbE5aYko0Sk9WZWNGbWg3SGtsdWhDbHVZQVFaYTU2ZHExS0ljcXF2aHhNYWp3M2xmQlFYZVZJSmVGQkVlMnUxMlA5WmNsRVdIcEtpclhqU3VXODhEcWo3a3FDd1BiblVrbVVWQzVwVmhTOWVIWHQ2YW9qS2FTbEFMbU9zSVNReW5Na0RhVVJwL0hocEwvOHkrOGxkWG8wMmV3RzlvMVhTMVovaTdwSmo0SG81OHVMaFRVckZBaXg4cDhuTDBpMnFhZzFjNkZLTVZRME9mN2RQazJ6OFlqRDNXZklaTWF2dlBzRzc3L3pDc0lrQ0pPaTlkek9vTFd4SUpCOEtLMEtHN211ZUFjcTNoT1RQMC9GNGFlRjR3d2lTVkU4ZmJyUDBmRXAzMzcvWGFJUU1JazNudFlGRjhBTEg4M2NyWWlBNFhqR3M5MDkzbmo5SG9IS1VxT3lnYm9rMFNteHRnRXA0L0djODhHUTgyNmY4KzZBZm4vSWREWkZxSUJhclU2alZpT0tJdXIxSmtZYnRqYldDS1FnQ2dMYTdRYWRkb3RPcTBtekhoTFZRbXBoVU5qZGRlRXd0UVBkeEZHbjNNcFhDQUtRaXhsa1hucUs4S2ZMd3NPZTVOTkpYVmhwUGZKNFBzVDBRR21pMVBKNzZUdStkdDhUL21SQVIydXNVT1hCVjRXQktrVjFEdWpSWFp5UlF6dWlqTmFhUmoyaUh0YnN5NDZvOUc4dVBNUVVUSDVqc3BGaWNXUEl6RWZoaERsK0JZT2ZGR2VFUndXNlFEeFYyWEtLVWh0Z1BEVmwyVEJWT3J3cmYzejI5V1NwalMrM1RHVmFzVy9SY0hyN3ZQSmJUc29Rbnp2Y1hkU0haU21GUlVpbWU0WU1kRS9QT0R0NFFVMEovdUZ2L2dxdjNOd0VQYmZxU0NOSzAyeWs4clpGUHZ2UlZHQ2Raa25XTWQ1L1kzSUNjNUpLUHZuMEVZbld2UC9lVzRRcWRiTWVMei9SZ1UveUlaMlhKbXlrcERlY2NOWWQ4dlpiYnpPUDUvUkdVNGJEQ1dlOUFlZjlBZWRuUGZxREllUFpCSU1pQ2tOcVVaVDM1cGZXMXgyZXdoQW9RYXZWWWpBWU0wdGovdDZ2Zm8yVlRwMTZGQkZJYVM5RW95MWRPc09MNmNSTDUvYlZxbDVscUMzVmV4bzdTNGZRbFlIUVJaTGE3TVhPVTJXczFOVXNHeTVkT1B3VG51aW52RElRcFYyejhPWURzaFNYbkNHNjgwckRlR21zd2l2dmNsUlNsbkpqY2xWZkVBU29JTXdYSGdVaHB1Q3h5U3lDeWR2Qm05TGRtYVg5eWh3OGtiRVIvTks1aEpIMGs1YW92dVRWRlk5Wm1MWWJVMktuZWRGZFJZbHBLaStsdnVEMU5GVmJkaDc4Nll0SC9DR3RLSE1Vdjh4ZkZVS09FZjZMNmp3RFNjcmgvZ0hEOHpPMjFqdjhvOS84SmhzckVXa3k5MVpjb2dqWHlPbkoyc3ZNODhqTzdubEkwWGtMVUVKSWxUWVdkdEkrU3pVLysvQmpWanB0M256dEpRS1JabW95L00yOEVSQ25Pa2VZeitPWThYakdjREtsUDU2eHMvT2NsZFUxZnZyelQ1ak5acmJNVjFhcGlaSUVLbUJsZFpVMXNZcVFna2hLR3JXUWRxZk5hcWRGcDlXaTNnaUpvcEFvQ05rNzZ2TEJoeC94ajc3N2JabzE0V3pZR3FQVDBuUHIvNDYxSndZV3BXQk5SN21Ta3NGTTgrLysvRWN1R3F5a2RGdHlqdnVtSGk4cXZDUUdvUUlNRVVzS1RsR0Jocko0OVFodmhVZGxPSjdkNXBuK1FJank2bEpROVFhWVhGQ1NhY0R6Vjg2cmNHVGV2aGZ0U3dFeWxaVzBRdUhaalF0NWNoYStVYVM5Rk1vK2swM0xYVmhvbVRHM29NeWhsT1JhZVp1S1g3cXJDa1I1THJCa3AzT1JBbURKdjI5S2dUQ0xhOVZsbklWbFg4eVVNaUpMYnRHYzAyYi9KNTdPMlh2NkJET2Y4OGJMdC9qT045NmlFVmg0aDZuYzI2SkN6L1dUb1lUeEJyQTVkNjhRS2hrUFlTYWRYc05TcGhRVG8vbnBoNSt3dGJIQm5SdVhFU1loTmRJNU5nTVNiWmpPWWlhVEtmM2htTFBla05PekxxZm41L1NHSTR5R01BcHBOaHJVNnpYRzR6R05acE5tcStrR2VWWmtWcSs3dnJ4UnA5bXdNNDFHRkJJR2xnMlpiWWlFbEdnVWo1OGQ4TW45Qi96MmQ3NUJJN1FyMGJ3YTlPTGZTZ1Bjdk5vc2F6TElZRGxTMFIxTStiZi80YTlRVVRQVEFaaUs3ZGYzN1pzTHpEdWkzRU1LczN5OXQ5UmRWRjB0Vm0zRC9vMWtxcnpsU2o5Y1hsMzZRYUNtdXU0eW9tenQ5bEtPY2pDbzQ5MVZZa2h6ZXBGZnlKWW9DVUtXek1nbHBnR1VjdDBXenFrdjZOS3JVMy9oS1N6OXlidTVvTXl2OGh5cUlOZWxvWExWaXQrcjZ2emYzUEl6WHVDbk9QdmZuNVRGNHpvYUREamMzWVY0eXJlLytUYnZ2bllIWldLMEx0cTZJa1dubUxWcC81TTJiaHZ2TE1UQ0Y3SjVnMUVoaXlvaWRkeEVaRUJxSko5ODlwQWJOMit4c2I3QytXREVlREtqTnhyUjY0OFk5SWQwKzEzUWtpQ0lrRXBZT2JaUXRGc2RPcDBWUjkrRmVpMmkxWWpvZEpwMFdpMmE5WWhHclVZdGY4bWR4aUNiWHhnRHhLUUpicmdjWU56TC8rbkRaengvc2M5di8rYXZFRW5RR2V4VmxLWENSU3VvODRPdlFLSGhOZGNTWk1EK1NaOS85NzBmMDJxdkVBUWh3VUtqeHBLSllPWXJ6bkRJWnNsTGE1WlBmd3QxWWRVSldJaU5zaDMyb3VyUDVLR01wZTlQVlBzR1VUNHdTdUtWTE5oVTUxb0NuVkZ3alZrNG9vU2g4cEZTd1ZaNHJZMHUwTTJGVGJNeTJ2dXlwZkxudTVxOHRzYXJBa1RGSEZXYUYzemVuRUVVWVNoK3k2UWx3cTI1eEFYZDRFSWVIU3dKMW5SQkgyS1pJdFRpMUUrT2p1Z2VIOUNKQW43bmQzNkRHOXVyb0JOMG9uTXFyODFMSmgvQ1p2RmN0dCtWdWM4aGI1V3lBOExacm0ydHBmTDlQa1lRSjVycGZNNW9QR1V3bVhKNGVNTHBXWTlIdXdlTWgwTlNZd2xLUVJoU0MwTWF0WWl0elcyVWxJUlNBUW5OWnAxV3EwR3pGbEZ2MUdrMGFqVENrRENVQmNoRUcveEFENEdHMUhqRDRlTDk4QWZnUm9aOC9HQ0hzL056dnZ0clh5TVNobFJyYjBoYnFFbnhocWhaMG5QSkUrS3BBTFVRUEhoMndGOSsvKys0dExsRnFDU3B6cVhBWldmWTRvc3NmV1oyT1pCQUxIdkdUR1dscUJmS2ZyRmdBRmoyZ25QQlRVV3gxc2ttL1I1dzBmL3pzcDVidW9OQ0Z4bEVudVRCbFUyVitFWC9DMnMvUzRDQ2ZGUzlNb1g0dkRuS0wvUFhzcmxBZVhlK3ZFMjR5QjR0cWJKcHMxSklPTCs4a0JZL0pqUWwyN0N2YXpCVXh6dmxrTmpzMjliQ2VHcmp3dWlsazRUOTNlZk1oejF1WE43a3Q3L3pUVmJyUURvbjFjWHN4TjdtdXZCdlpPczJWOUVwNmMweFpKRXFsUmhJa3BUSk5HRTBuVE1jVGVrTlJweDNlL1FHSXlhVEtjWm9WQkRTcU5lcGhTSE5WZ3NwSk8xbUM1MWFDR2VqVWFQVmJOSnAxZW0wV3pUcUVidlA5bmpsNVh1MFdqV0gybkpyU0lmZXNyRm9GUjJGektoTEZpWnJTY3c2bjZIbFBuMmhRSWI4L09QSHpPWnpmdjJiWDBYb21DVFZIcXBlT3NGYk1ja1h3aGVDU1lkd3Nob1RneUNacDB5U2xBL3ZQK2FuSDk1bjg5SVdnYkxhZ3BOdUwyc0JMbnBlemRLSHkxeDBzUzMxSm42UkdtMmhxMkRCc3Jha3ZmQWZmSDlvdG55bzVuMmRCWkZOeFJkalJNbFo2UFBYODVETkRCNXFCRHF2RStTWFh1aDl1WmZmZjNGMTZUYjVUM2FvWk9zLzN6dVJEekFyNGlULzI1SkZRbzBRQlh2ZnR6K1V2bmYzS3h5UGhoenVQaVBRTVY5LzV4VysrZTVyMWpLcjV6YVBJQmZGV0xlN2RvcFNMZktjY3J2SGxvcDVhb2huYzBiek9aTnB6SEE0cHRjZmNucldZelFlazZTMm9ZN0NpQ0FNQ1pTaTBXelJicmNKcEExanFkVkNWbHN0MnUwbXpXYURacjFHUFF4dzdiaXRGRlBOTEU1NCttU0hWKy9kb1ZsWGR0SnVkT1dTSzNJelNpdFE3VW1vcFNtdHJQUFBYTmhaMDA5LzlqRmhHUEwrZTI5WVJvTHpHaGhUckdsdENTVlJLSXl3RWV0cG1qQ1p6Wm5NWW9iakdjUGhtRzZ2VDdmWFp6eVpFbXZOTkRZMG15MmlVS0cxNXF6YjV4dS8raDJDaTgwaS9rdHFGclg3cGdwQUw4d0xDOVNSQzUvYWluQjFXYXNnek1YblVRVkpWbGJWVlpTTjd0dFZQandzNTVZdTJWZGpTaFBvMGs4di9Pano3RDlJTVViK0o2b0EvUDgrdlhBcnMvekFvRFFQWHZvNWUzNHlPN1Iwclowd0MvTGUvQ3RJVHdScXlyTGxVaVJsTG94eXdvWEc2Z0FBSUFCSlJFRlVJRXozS3p3L09lVjBmNDlJR243bk43N0pxeTlkY2J0M0RVSWhsTFJPUWh1TlkxRm5CdWJ6bE1sOGJtL3o0WmpCWU1oNXQ4ZG9PQ1IxTFppVWtxaFdJd3lzV0d0bGRkVzFnaENHaWs2alRxZlZvdGFzMGF6WGFOUkRhclVhVWFBSXZhclJIbXFwUlhscWU1a05SbE1lUFg3S2E2KzhSRDF5a05PUzdGZmtObmlSMTd1WkR0YXJtRE5wUE1JS3FiTGVYRXBpby9qeDMzekV4dm9hYjcxNngyb2VqQUFaWVl5MlE4ZzRZVFNkTUpuT0dVOG5qQVpUK29NaC9WNlBlRGEzWHpOUTFNSWFRV0FEV2FNb0pJd2ltMUoxMnFYZGFwSG9sTjVneEJ2dmZwMy8vRC83QjVZSmVGR2Z1QVQ3K3ptdEFsOHdlMTV3Nnl6WkJId2VqdXppRjZwWTBZbEZJVXZweGhjbGkzS21Rak4rL2JyUXQxSXgzMWErcnNkWkU0SXYvRjR2L2p5V2ZDMHZxYmxjRFlpbEI0QW9qVC8xNS81T0ZvMVgwbEo1RjM1SHdpdmxSVVY5Y2RGeGJncUZtekVjN0wyZ2YzckM5bnFiMy9uTmI3TGVxWk1tc2FVVkcydWJuczVqeHBPWTRXaEV2ei9rcE52aitPU00wWGdHQXNLd1JoZ3FRaFVTaG9xMWpRMmtsTVNUTVN1ckhScjFpRmFqUnJQUm9OR28wNnhGQkFFT2YrYnlHelB3Q05ZYllKS1V4QU9VRmdJeU81VWZEY1k4ZVBTVTExNjVTNnNSMmxRaEo5K3RpcXo4K2JuMGpYSkNsNm90a3hGK3NRVHFWRXQrOU5PUDJMNThqUnRYTnpnODZ6T1p6UmlOWi9RR1E4N091dlQ2ZlNienhMcEdWVWdRQk5UQzBNNENWTVQ2UnR1MlJBcENxWWlDZ0hvam90MVpvZFZhNVMvLyttOXBOZXNZb3htT0o5eTQ5eHIvMVgvOVg1SmFJcEMvN1RPVjNuekpJZkJMMXFEWm4ybjhiV3J1Z0ZwUXdWOVlKVlFXZzB2YkJQdXlwTjZPTjF0SldyZVZFRm1FbHZGVXlxYVU4bEtkL2w5Y3lSZ1h3UEc1WExOZmF0QlhWZDR0ZnRUNmdyVmhWa0JWdHlwbVlZUXB2Vit0UHdnc3BLcWlHTDZLWWkyYkR4Mk5jVkhacHNUU0w5OFBib2lheE96dTdqSWZEbmoxN2syKy9mNVgwUEdNdllNeitxT3hMVlc3UFFhREFYRmlHWUZSVkNPTUlrS2w2SFJXYUxVTVNtZ0NONUJyTlp1MFdrMm1zem16OFlpdmZPTnRvbEJabXE1TEViWlJZdmJHMVRyejZoY0JvTG4xM00wcXJGUFV5N3hFTWhoT2VmcHNqM2ZlZXAxR3dNTEdLcWNXaUpJQndRRk5LYklKaFlYQ3BCcm1TY3BrTm1jOG1UT1p6QmdNeDV5Y25YTjBjczd1aXhQKytpY3hVZ1hJUUJGR0VVR2dxTW1BOWJWTHJHWnFRcXlFT0hWWmpLKzljcHZOOVE3dFpvMGdDcWtGRVZFZ2tZRWdvY2IzdnY5M2dDSU1Bb2JqS2V0YlYvaTkvL1ozQ1pTa3ZiN3Vid0ZNK1FHWHhndlZxK1lmL3hKZHB5aC9kSGpHaHdJWXFvdFMxNVNIVDJMcG5TcVd0Z25HejQwclFFeEZCa0IrOTVuSzBteVpOdUhpRVFjVkl1dXl5dWJMSHd2bWN3OVB2ckI5S2drb0x1ajVLMHBNVTN3MitaWkVlSUdVM2xERU53WDdDWmpGWE1BVWVnZlhGa29FMDlHVUY3czdpR1JHTFZBTSszMys2Ti8rZTdST2tVRm9HZlpCU0swV3NINXBnMEFLQWlXbzFVT2E5UWFOV2tTOVhpTU1GUFV3UWdiQ1Jvd2plYlovVER3ZDgrNmJkMUhDb3JCMUlpcTJkVjFhKzJhem5DeE4yQzZoaXVkR2EyUHpHR1RJV1hmRWk0TmozbjdyTlFKcExBSk0yMHhBNnpDMXRqQXBMRmN3eXlKTU5jd1N3MlFXTTU3YVhydy9tTkRyRFJpTVJrd21FMUpqaUlLSU1BcUpvZ2lsSkp1Ymw3ekp2YmJsZXhqU3FqZG9OU0lhalFhMUtLUVdoUVJSUkgrYzhLTWYveTEvNzV2dmNtV3pqVFRha29LVmNtR3Fna2tzK05Qdi9TWGphVXhVcXpNZXo2aXZidkJQLy9uL2hJb0NHcDBPNzN6bEs5NFdvRHB3TXhXbm5mamlRdmJDenJkRWpER2w5RktxcEp2UHFUQVdlTVNDa21KdFVXRkh5Wk12WlJZQzR1Mnl6WmMvMTdKYldac0tUT09DS2FyNUVxS2N4VS9PTEZSYjFYOWlDaXh4YVFKdjhuQ1ZhcHRRRVMySnltY3BvTXFVb0J6NDdMQVJwbFNQaVNyVDBmWFI1eWZISEwvWUo1S0dtOWN2MDRnaXBOQVkzVUlJUXhTRk5KdDEycTNNbjkrZ1VhOFJoaTY5TnA5NmEzdUR1OVZ0cWdXZlBIeEtveGJ4OW10M0xHMFltOFFjQ0ZreTZtUnJzZXo3dE1OYm5hbTdDa3RVdmozU2FFS09qN3VjZGJ1OCtmcExkamR2SUJYS0prQUJjV3FZVDJLbWNjeHNGak1janVnUFJuUjdmV2FUR2ZNNHhRaFFNaUNLSW12QVVZcG1zMG1uM1hMM3FDRUlvQm5WbldDb1JyMWVveEhWcUlXU01BeGMrSXZMTGNpRVBVWndkRDdnYjM3eWQzem5XMS9uMG1vRDRURDBCc3VqbERKZ09vTS8rWTgvWkRTSjZiUmJUT2R6UkwzSi8vejcvNHg2czBZUTFYam4zWGVKNmpXQzhtamRMTjE4VitkYXhwU1ZhdFZiT2kvN3plSUFUNGlxYU45NDBkeVZDeTlYTkpVOTE4SVh4UHRmd0ZOQTVXRFNRS0dFSWxDQ3FCNXdwcFM3S2ZRdlhkQm92OU91L256TFh1dWwxNzVab3ZxcjVNMlhweGJsZUxOTXltT1c2Q2xOZGVVblNzcThRcVVveWc0OHJ5b1RKYk5SSmozMk5QOSthcTMzQzlOYVE1cXl2L3VjY2EvTFdyUEcyMis4eEVxcllTR1p0WUFvQ0owanpjdHJKSXZxU2pGcG1sK0VtZmduRUpKVVNyU1JmUERSWjF6ZjN1RHExZ29xVjM3YUd6MTEwWElGWGNlMmFKSXNkRmE0MUNmY0phQWQ2cjE0eHZhUHVnd21NKzdkdThka09tRTZpeGxOWnd3R1k4NjZmVTdQenhtT3BxUmFFd1VoS2d3SmxTSUtBd0lWMGU3VWN2aXRGSUlvVUZiMTEyclFhSVJXK1ZlckVVWUJRU0J6KzdpdjRqUm9wTkhPN2VnU2k2UkVpSUNqOHlGLy9aTVArSzNmK0RhdG1nVGo5QnBJMTNvb2VoUE5uM3p2QjZSYXNOSnVNNTNQbWFmd0wvL2dYOUJzTnhCQzh0V3ZmNTE2bzRiRytFUEE1YVhwc3JtVytJSTVnQ2pCQ2sxSmg3eHN5NWRERUJhMEFwVi9WMWJtVWo1Q1BFdk1OZVYvS0pJRXpBeXRVODRHUFo0L2ZvZ1NLbWZMRzJtV3p4NHZMRU4wUlNENXl4d2pvbkxqcDU0UVNpeCs1dGxkYnJJNXZmYTg1NW95T3YzenhiK21JaGsyUG1wTVpBeEVXeC9iMGxqaWcxcUtDckhzNThqU2JlZlRHZnM3TzZUVEVhL2N2czZ2dmY4V3JZWUNyVjBJcHZOa3VERFlFdS9WR0dLaGMvZWxaUk81cWswb1lpMzQ2WWVmOE9yZDI2dzJRcVRRdVpzMFA2QkVXZnlDQzN2SkR5bUgwdGJBUERITVpnbmplY3g0UEdVNG5kTHJEVGs0T0VZR0FSOThkQjhsQkVFVUVvWjJzNENRUkZHVDlhanVRS2lLSkVsWjd6UlpXMm5ScU5mZGhpRWlEQ1ZCb0lpa0xOUjVPczFwdjQ0M2hFbDlCcVB3Nk5NRlZjbWdNVExnNktUUFQvNzJBNzc3NjkraTFRalFhWnhyTXF6aFNUS1lKUHpSbi8wSUtRTlcyazFtODVqaDNQQXYvOVVmMEY1cDJaZi8vZmRwTmh0NVJGOXdZYlgrSlo3clFuWXJ5Z0lkV2J3bzVldmZrNlBtdmFkWGtoclAzWit2cEFyeXJQQXRmMW5hamkxK2JPaUZnZmxzekxnLzRPemtrTFBEQXc2ZjczS3crNHpQUHZvNUx4NC9RS0FKbEN6TFhyL3NqTjRzNERZdS9MZk5oVTFCVlhicmk0ZVcvOXVMS0JFdWtCT2JMOVdrbVF2enI3T1hSZVV2ZHFsZ2N5S2hmQnZpRW5QN3ZTNHZuajBqMUNuZmV1OU52dkxxTFFLeTZQSkM3ZWxEV293M1N4RGVneXlFd1VqTFlCQXlZRG96ZlBEaEozejFuZGRvMWl4YTE2U2dwU1VHNHc0cmV4RGFvRmJoM0lOeG5EQ2R4b3ltVXdiRE1jT1IzWS8zQmtObWt4bEdLcnNsVUFxbEZHSGRyZ1pWcTBFb0JGR29xRVVCdFVaRXZWNm5XWFBPdlVhYmcrTnpSdjBlWDMzelpXdjUxY1pLY0VSWnNZbk8yaTZWdzJnTDI3cFpmQXlkNWtIbnZ5M0Z3VkdYRDM3K01kLzl6cS9RaWhSR3h5VW1Sb0xrZkJ6ei8vMzVEekF5b3RWc01KMG5uQS9IL01ILzlxKzR0TEdPRWZDVnI3MVBaMlhGSXNuY0x5VVF4bCtEZmJraGY1SHU2eW05UExWYVRsd1F5K1RGSHZqQmVDejYvRW9vUUNMYTJNeEFJYXdMU21vTGxWUkdrOHpHekVjamV1ZG5kSStQMkgzNmhHY1A3L1AwL21mc1BYNkFTZWUyMUhQZml3Sld3Z0NqUWtiekpNOVlGMllSKzMzUjNTNUUyWDlnU2h1TUpYNkhDK2NMcHRRbWlDOHhJeWh0YUxJL1EzOXhOVWJGQWJENGxTb0hWMlovOWYrSlQxcHpMRU1MUlRFYzd4OXllcmhQSXhEOC9WOTdueHRYMWxBbWRua0JiZ2dyaEZmbEZjMmx6T09xQ3YxbERtVVhpc0VvNGY2akozemp2VGNJQTd1bUZFcWhwVVNuTUU4VDRqUmxPcGt4bXN3WURFZjBoa01HZ3hIanlZUWtUbEJTRWRZaUloVzQzanJnMHRvNllzMTlQV1ZMOVZ6U0d3WFVhM2JnRm9VQlVhQ1FLbU5OcENBQ1BudHl3R3cyNDUzWDc0Q09uWUlSVXJjUnlGS2F0UzRjanhsNElQT01aSWRBTnEvSlplbWUzMElMeGJNWHA5eS8vNVR2L3ZxM3FFYzJzY25PTkhJQ0pXZTlLWC8wNzM5SUdFUjBHblhtY2NwWmI4RHYvNisvei9ibERZd3h2UDdPVjFoWlczR0oyTVZqRUJoaEZnZUFYL2d3bGZvQkZqbTJTekJVcG15TUtidlIzY3RJMXF1QkVxQ01aam9iTWVwM09UMCs0ZVJnbjcyblQzbjI2QUY3VHg5ejhPd1JJb2tKZzRCSUNrS0g4ZDZvSzR4b1dHRkVaUGZIdFRBZ0NBSW1zNFNIdXdjWHZlRmZ0TlBNZCswK3ludlpIeWJOUlVwblVUcElvYUlQRjh0MUFabVJ4SU5JWHlBRFhqd3NGcHNMY2FHK3dnSEl5M01KUThrUktoR1FwdXp1N0REcW5YUDUwZ3EvL1d2djA2a3JsS2RhTUg1MUkvRk1xdHBOMDBYSlVxRzFJSEVSWmljblE0N1ArcnoxeG1zTUp4TW0weG1qOFlUaGFNTDVZRUN2UDJRMEdqT2IyN1Z2RUdTKytvQlFLZGJhYlh1ckswa1lLcG9OcS9TcjFTTWE5UnIxV2tBWVJnNEQ1cldCT2kwNlNKa0ZwbVFKV0FFZmZmb0VHZFI0NysxN21IVG1YbHhWM0F0T3NheXp6WXEvS01Ma2w0TE9mUUFTNFVoWHVEVzFOcUNONU5IT0lZK2U3dkxkWDMyZlVKbE1TK1FreFJJaFE1NGZuZlBILytGSFNGV2oyV3dRSnduSDV6MysrZi95Kzd4MDl3NkoxcnorNXB0c2JXOFYyNi84b3BOZUMxQXgwVnlZRW1qS2FySEZRWmhabU0xWldFS1c3eVlkVk1HV2JJRzBka2t6bnpIc25uTjJjc3poM25OMkhqL2swdzkvenNIT0UvcW5SK2g0YmlmRTJwSmNvMEJ4YzYzSnBmV3IxQUxKeHVvYXEydXJUR1l4UjhmSHhLa20wZTQyY3hrR0pvMUpVMDFpREpINWZKbVJXTEtHTTBKVWh2VGVFV1kwQzNsMUMrK3hLWHAzTitpcXRsSG1vay9lVU83ZlAyYzFLTVZGeW9KcXF5QzlPWVFwY1FSOHQxMzJlTGdRWGViaktYdlBucEtNaDN6bDFidDg2OTNYQ1VXTTBZbVY3MmJQaFRCNTJJenQ1d1ZhQzFCaDFsT1FKQW16SkxZS3Qwbk1ZRFNtMnh2d2ZPK0FXaFR4MFNlZklxVkV1UmM4REd6WjNtNjJhRFdhcEtrOWdKVXdySzYyYVRick5HczFhbEhnbkhpUkhiaUp5aWpYWUZWL0dROVJXK2t2L2pncWoxUVhvQlFmZnZ5SVZxdkdHL2ZzemEvelFaY3U2U2ZzbnljOVVKTC9YR1JETEoyekY0elA3emVBRFBqc3lSN1BENDc1N3ErOVR5anNRTERFZnBRUmo1K2Y4T2MvK0dzYURmdHpKMG5LMFZtWC8rR2Yvak5lZS9VVjBqVGw3bXV2Yy9uYTFmS1dPTmZHUzM4SUtFdW5makZOcmR6OS9ndXhJQWYyaGpBWnhWUVk3QkpGUUJLVHpHZjB6KzJMZnZUaU9ZZTd6M2o0NlNjOHZmOHBzMzRYMGprQ202RWhSRW90RExqYWFiTFMyYURUYXRLbzEyblVJMnBoUUJnSTRpUnhDVEtTdmYwWFRHZXgrMFdxWEpGbHZKaXQxSGhaQjFYN2UwV0RVRUxYWm9PeXlrcDBFYmhsUG1jMVdMWXFhZU9CSkwrTWZMZzBSeFdmcS9WZndobGFMZzdLVnc1ZTVMWFFDQzNLQkRMbmFPdWVkamw1OFp5YU1IejMxNy9CcTNldVFqcXpGdDY4NDNMQmFVSWhqQ0RXRnB3eG04d1lUdVpNWmpNclkrMFA2UFZIek9ZeFNFa1lLSUlnSkF4RDF0YldDQU5KRkZwbFd4allISUZXczVIZjRyVmFqZDI5QThiakVhKytmSXRhSVBNak5CODBpaFNodGFNRDJCYXlkTVI1MlFuU2NTRXozSnR4TDNTYUdIN3g4UU0yMWxkNTdjNVZURHJQZzB4eXNLaEljNUNOeVFlbE9ERVNlUkowanM3RDV4ZGtmRUdEVUNHZlBYN0IyWG1QMy9pVmR4RW14cmpQTVB1OXBWcXp1N3ZIai8vbTUzUldWcWlIZFZKak9EbnI4ZC8vM3UveDl0dXZFYWN4dCs2OHhNM2JONjFPb0FMS1FVcDBuR1FIZ0EzUUxNd2h1cno0OWg2bGJEK3BuUTlkT09DakZCb2xzUjkySEJQUEp2VE9Uamc3UE9SNC93VXZudS95OVA1bjNQL0ZCMHg3NXlobkloR3VwNm5YYW13MUc2eXVYS2JkcWx0MmV5MmlWWXRRTWx0emFCZW1ZSXZ3NldUSzNNQjV0ODlrT3M4eitveVBieExTZ1V3TFg0REo1YUFlNmNwVVZtSjVLbXZsbFJHZkwwZXVLcVlYRmMwaTkxS1laZHZNTC9RSWllVlNueEtadDlyM2Z3bVpkcVpjTXk0YU1HdDVNbytGMGV6dkhqQTZQMkcxSHZJUGYvTmJYTjFheGVpVVZGbFo2aVNlTXhoT0dJMG45SWRqK29NaHc4R0kwWGhNbW1xVVZFUzFHa0hvQUpWQ3NyYTZhbHUrUU5tWFBiTHJzbWFqVGpPcTBXaEUxR3FLS0l5c0JGZTRDc2RJZG5aZklIVENtL2Z1b0pUMXdXWGJEWlVIYlFySGJzenlhckpuZ3J4Q3llQUR1WS9CemJFVERYTXQrTHVmZjhMMXE1ZTVlM01icmVQeWpFVDRLMU4vYUtwemhCeEtJandaWnFtNkZDcS9MSVJVM0gvOG5JUGpIdSsrOHlabjNRRjlON3djak1jTWh5T200ekd6ZVlLUmtrYXpRNnRXSjlGd2N0Ymx2L2pkMytYOXIzK05PSTI1ZHVzMkw3LzZhdVd5dG0wTVNFU3ErZlJuSDdob3NOSkpYeWtlODZobE43RFIxcCtzQUJNbnBMTXh3MzZQM3NrSnh3ZjcvUCtWbld1c1p0ZDkxbi9yc3ZkN2Y5L3puak5uN21QSGpqMitKSGFjSmpHOUtLaFZBVUVyY2E4b1ZWT2FVbFFFRVlVQ0xaUWloSUFBaFFwVUpFREFCeERpSWlHMVNCVXFFcGRLVGRJa1RlellUdUpiblBIWUhvOW5QSmR6ZVMvN3V0Yml3MXA3NzdYUE9HbkpsNW1NYmMwNTc5bDdyZi9sZVg3UDFkZGU0YzNYWHVPYkwzK043T2d1cVZRbzVROEc0U3pqTk9ITS9vTDVkTXBvUEdBMEhESU14RklWMER5aXllZ1RBbW1EL2JGMW9nbnEwcElYVzlaNXdmRjJTNFBudGM1NU1reXplcUhKSjVCSUo2aXhmVVIxTDhGTWRBanRCaGdoUks4cWlGV04zMzVVOEI1bHZJRFdxaGtDS1RvZ1NieDZzYityQVd4UDJCTXpHT001ek85aW5OczJIZ0VzNTNwS1FmOTNtYkxpMnB0dlVtVWI1cU9VajMvWHh6REc4TkpyYjNCNHRHYTkyZmg0OWRxQURObUZ5b05WMDhHUTRXaEVYWmJNSmhNbW95RnBxaGlQUisyd0xkRUMzV0N6bXJvYjI4Sm9oYkJZVTdaQTB0b0pYbnY5Q3NQQmtBZnZ2OURtVjdxbTVSTUNhYnZ5emdVN2QvUC9Pa0t3NksrVmJaZnNVVHRIYVFTLy9aVVhlZkNCKzdsMFpvNnhwc3Vua0gxT3Y0aExmQ2ZiTnNpRWk4WUtUemMzeGxFWlExbFVGR1hOdGlqWWJnczJXY1ptbFhIMzRBQXBGZi9yLzd5TFZKb21jRUlvUCtNYVQrYWtJOHZoYXMxZ01LQzJqdHVIeDN6ZkgvcERmTS8zZkNmR0dVNmZ2OERsUngvdERxZG9taXVFd0ZuRDZ5Kyt4Qy85bGIrTzlnZ2lGVUkzVExzUmtPRVVsY0pIT2VlYkRkdmpZOTY5OFE0MzNucVRxOS84QnRkZXY4SWJyN3pFNXVBV1NvU0VYUWRhQ2dhcDVsemd0czltTThiakFXbWlHQTBHUHNFV2dRbVVFK0ZpeXE3b2VxRVRqSDJMWTdVdXlJdUNiVjVRVkZVSEVFV0V0VkJ6OG5hZXR3WkwzWTJ3WE5mekVxK21PblIxUnpRVy85K0NJWEZTeWhzT0ZrR0hETGRDbkJnK3V0L0JZSE5peWRpYXNycXBjdjl3RU4vQ05maGU4d1hSN3NyYnp5MDQ4NDZPRDdoMS9XMkVNU1RDb1NVODgreHpESklFblRaQnBZTEpiTVpTU2xUcUZYQ0RSREVZakJpUHhseTUraVlYTHA3aDR0bDlyMnhyYy9BYWhWdEV5QkV5c1B4bFZ5NGJoNVVXb1RSMVlYbnRtMWZaUDdYTHFUM3YrbXMwRksxQU02UTJpWHVFVUxTc0FoRU9mQnVreXcxTldFcUpkWTZxRXZ6MnM4L3oyS09QY25wdmluUW1LcVBEVXhRUmoxd0lSYTFyUTJrTWVXRDRiN0tjYlZhd1dtMVlyVGRzdHh1cXlxY0JLNlhSaVVZbG1rUnJobHB6NnRTZUIzN2lRczZCUHh3SHd3SEQwWmhWVnZIOFY3L0c3dTRlVGpodTNUM2l1NzczKy9sOTMvOTlXRnV4UExYUFl4LzRRSnNlM092NThZbkkxMSs3d3QvLzZaL0ZYSDBMalRObzY0TUR0SE5rbTJPT0QrOXcrOFpOYnIxOW5WZGYrVHBYdi9FcU45NTZnL3p3TGxJNlZEanhGRENTa3QyOUhVN3RMWmxOUE85c01CaWdRN2lsY3c3akRNYlVKTXFuWTlvUXIzelM0aUpkK0VIRUZLRWc0MXpuL3BUTWlwcXE4aUlJcVRveWJMZk9iRERJMFlNbWJIY3JDOXVXNFMxZm91bngyNHFobVFBUlFrTytmWGQrNzlROWRremFqdG9pK3J5ZWs2K2k3QmxJWFZlVlJQZHgzMnpkaFVTS1NQc2V4NW8zTDhlOXVHL1JUMTBXSG5FdW01TGZHdDY5ZVl1N04yOGluZUhTMlZQczdzeFFDb1k2WVpCcUwzd1pEUmtNRWthcGQ2bUpnRWYyWVJhQ0YxKyt3Z1AzbmVIMGNvcHdGZFowWmJJVUFtZWozSWNUV1lYTjkyRERqR2U3TGJoNjVTb1hMNTFuUGg3aVFrYUJ0eDJMdHYrV3JqKy82VWc1NGg1QXFRc0JNSW91eXJ1eWttZGZlSkduUC9JVXM4a0Fad3hXS085WXNaYXlxdjFNb3pKc04xdFdtdzFIeDc1RTMyWmI2cnJHU1QvUEdEYURTNlVaSkFNR082bFBpd29WWmFJRjZUQmhQQnd6R1F3WWpkTUFLdkhNL3laRXhBakp6WU10bi92eTU1ak5GaWdwdUhYM2lJOSs5OGY1d1IvNEF4aGJNWjB2ZVB6Sko3elNzcUVDQjZPU0N4WE84YnUzK2NlLzhQZll2dndLRHl6bTZKZS84Qmx1WDMrYjE3L3hLbGRlZnBGYjE5K2kybTU4ejIxcjc2RERNZFNhMC9NeDgvbmNEK05HSG40NEhDU2MyZHRESzc4YXNpSE0wUUxXMU9SbDRRbThVbm5BUVl2dTZvK3MvR25jK2FpYnRKaktPZzRPRDhpS2tzclVyWVczMVhmYlRrM1M3bXJiKzlTL0hOTDVnWlNTemdkd1JqTzZIazZ6NGZlTFdKMzNiVzc2Q1BYZENtZnV1VytiRlZLNE40THVYSjVZeWJsNDhPZ2FlNjdyWTlPaTBqOWFCdmI2VVhjQ2hPTGlqY2JKV3VJRUNsSklIMWdxYmMzVnExZlpISzlZakFkOC9PbVBzcHdOR1dpL1VsTkNJcXh0QnZuZENDR00wSjBVMUpYa21hOThuVWNmdXAvbGZCUm1PUDBqejBaTzBkNWhTa1A1dGY1WnNvNkRveTJ2di80bUR6MXdQOU5SMmxseW95bDdJeTZUUFNKeW1BSEUrb3p3VzBNREI1VitoeThrV1ZIeTdIT3Y4UERERDVHVkZUZHUzK1h3YU0zaGFzWGgwUkhiSUFWR3lIYU80WEFNMDVUWmJNTHVhSVNLTmw1SkVCT05odDd2TUJqNnd6Tk5QQ2ZRdHo2MGNKVm1SS2lFYUtzakt5VHYzRHJpZi83Znp6T1p6a21UaER1SEt4Ny9qby95Ui83d0g4VGFtbVE0NG9OUFBlVmoydHZjRGRyWUpZZGhjL2VBVC8vODMrSG1GejdQcGZtVVphb1FqOXgzemlXSjkwd3JJUmltbXZGb3hHUTZDVDNhZ0VFWTBNZ1dNMVJ5ZEh6RWZERm5QaG4xSG5RWmlLVlpXYkRaYkpqT1prR1FFMWxRZTZMWU1FeTBBVWdSZmtMV0NkWlo3bFZiVlFYSUlPRzFnZUlydXoyNmk3dCsycEFNWnowdHhWcUhxUTE1V2JES1MrNXVTd2FUSFZRNjlBOUhtNGJibGV1L2szVW5GZ09kMU84M3JZc1FNZG84aEhDSzdyYTN6VUhXL0Q1K0laeGZGYm4zSE9LN1BqL0I5VHBRdXNTYnFEOTFKM0laWEs5Z29NdzJqRkxKSXcrL24yKzg5Q0xaWnNOOVovZjU3dTk0akhFaVVHRklITlN0VFNvQzdiY1U5dmNvelNhdmVlR3JML0hrNDVjWkRUb1RUdnhNaXFqemFCRzNqZnJUZHVZZEt5UzM3aHh6OWMxclhIN3dma2FwUmltQjBpcXc4ZjI4U0FSWG5tMk1yTDBGcTJ5OUFiVzFQdTQ5TDF1S3ptYVRzVnB2V0sxVzVHV0pGQW9oOEllaDFrZ1ZOa3BTaEVyQi8xMUtTcklzUnl1ODZXbVloQ0FQN1dFY1dwT294alhvWjJrdXJBeEZMN1FrT3JTbHIvSmthS210MUZ5L2NjRC8vc3dYR1U0V0pJbm1hTFhsd1VjL3dJLzh5QStodFVDbkE3N2pPNytMMFhBUTdmcmpkQ1JCdGw3eHovN3VwL25xci80cUV5VjRjTEVncVdyMEUrKy9qOGwwN0VVU1dyWDU0ZGFGYU9iZ0JiZjR1T2FqbzJNc2p2MzlQUklWYnVCQXFuVkNVRnM0V2gwamhHQ3gyQ0hPRi9TOW1HeEhNaTByem5vRXBMUVNwQ1FyQ2c2T1YyeTJCVlZ0U05NazNEUTJoSTUxZkhxRHc5VFdwOGZXTldWVlU1VVZaVlg1UERncFVXR0ZsS1lEWmlyaFlGdWVtSXpMVmtCelQ5UzFjNUZ3S1pxNGZ4dlgzejIvdjBkaEdNcjVHSFBkcWlQcDVmQjE0UlpkZTlTekpOTVlYK0tXdnR0TGQwUFBybXB3MXJVVlNNT1p4OWFVdWVYVnI3K0lxSEsrKzhPUGN2bitpeWhYSVZ2ampjVUcwWXB0L3N3S1VPR1FrWnJiQjJ1K2VlVU5QdkxVNHd3MEdGdUhHeWcrQVpyOWl1MFE1NktyZXBva0tDRVZiNzl6aTF2djN1YlJ5Kzhua1JLa2I2bjgrdC82TmxENDdVOXRvS2lOUCt6emtxd3N5ZlBTcXdRM3ZrUmZiemJVeHM4VGtzU0ROWlRXYUNVWlRTYk1GenRCbXlMUVdwS0cvandKc3cydGd6b3dTYm42MWcyU0JKNyswR01rS2x4QnpYbHZuUStSdHFiOTgyYncyQjN5L1NxeEFacjZFOVlDQ1c5Y3U4MW5mdXRacG9zbGFaSnl0Rmx6NmYwUDhjTS8vQ2Q4WkoxT2VPcGpUek1hRG5xUVdpS05RcDF0K1EvLzRsL3g2cS84S2hQaEdDa1YxdXdPZmVIMExrS0pvS1gzaWp3YnViU2NGRmhqS1hQRHdlRmh1UFhIdmdSei9tRnJGQ0pGV1hMbjdnSExuUjJHd3pSa3lYVVN6MVowUWJmL2JxOEJCMFZ0V2ExWGJMS2M5VFpEYWMxZzZFa21QanZlWjhpWGxZY2xsa1ZCYlh6VWNxSzFCMFBvaE9sa1RKSWs0WlQydloyUy9sYlpsS2FOaE81TjlyOVZ1czBKNUxaejdyMEZSSzQvTUJUaTNraDBFWWxlWmE4c2IxN0dicFlnTE5IVXVsdko5amVCSi9QaVhjLy83cXhQb2pYV1o5R1hwZzYza0d1ejdiMEt6clh6aDhFbzVYdC83MGM0dHpmRGhwYXJPNVRDVENLazQ3U1VXaUVRS3VHTmE3YzRPRHprbzA4OTduMzB6bmpLY0VqK2FjMUY3YThxYXJOc21KNTMyS3czcnQxa3U4MTU1TkhMN1VkY1crR3R1S1doS3Z4TG5tMHpOcHVNN1Raam0yZmg4L0F2aHkreEJWcHJkSkt5dXh5RVNrT1NhTUZna0RBY2VKZmVaRFJra0NaQlo1S2d0UWd4N3MxMDMySXQxRTd5dFZkZXB5b0x2dk9weDlES3RhNDhKem9scHJQdVhyMk1pKzNUM2FDNVI1T1dDb3ZpNnJYYi9PYm5uMkd4czJRNEdMRE9jbmJQM2NjblB2RWo2RVFqbE9hcGozeVV5V1FTSGFMOVN0RldOYi95SC84enYvMXYvaTJuUnlPY0ZCaFRrWVNmcDdaaEJlUWRXODBxVExZdnFhMGRCOGZIbU5wdy90elpzT3R2dHB4KzVXR0J3OE5ENnRxd2Yzb1BKYVIvK0VRYm9JUjFqU0RIdENXeE1ZNnlyS2lxR21NdDYyMU9WdFljSGE5SWRFSzlMYWlxQThxcUFvUVhpaWl2QkJzT0Jzd25JeC9lS1hYNDJqMzYyOTNqeFEzT01kdmdtaU45ZWt3UC9sMHdDVm9WNUQwbTNxNDE2TjM4eE9uTHREam4yR0xiSERITjUwUjdQMGJjZy9CNzAvVDlRZUhvckdmR08ydDliOXI4R3NyTjNsRFMrWldxZEtDa0lFbThXbTZZYWhJSmw4NmY0ZUg3TGpBZGdETW1PclI4Ymw3alMrODJMc0pQNStXUWw3N3hGbG9LUHZTQnkwaG5mR1ZodXdPcTUyTU1tWFZoRXVhZmVTbkRJZVdvcmVQTmErOXc1L1lCZTZmMitPb3JWemhlYmRodWM0cXl3RmhReWl2ODBnREtTTFZpUEpreG5jNUR5Q2NvSlJra21zRmd3R2dRT0lBREx5eEtkWXBTb2U4V0hlU1U0RndVR0Y4WWhiVTBnU05obk9TWjUxNUNwNXFQUGZVSXFvSE11MjRnMjVxY0lqVDZ5UmxsZi9pUUFBQWdBRWxFUVZTbmNCMWVMWjdQT09lb25lRHEyemY1N0JlZVkzZHZ6OU44c3B6QmZKZFBmdkxIU05JRTR4eFBmdmdwWm90WmV5alRhbm5DNTI0Y3YvWXJ2OGF2L2NOL3dsbXRPVDhiYy9ONDA2cEZ2ZUJPeEJOUjJ5cWxoQkJrZWNYQndRR0xuUVd6eWRncjRWb0JnNWRQVnRidzdwMDd6S1l6ZGhiRGJ0VVZaWEhVNFdTM0NNcmFrT2M1NjdWWGdKVmxUVkdXYkRaYnFzcGpvUVpwQ3NveEdnNll6OFpvN2FPNHdXS01DWU9PSmpBMGNpSllkeElkRUZES0lUUWlDanZ0aFhXMFlaMGRpeTlHZnNmcGM3NkgreFlldkY2Q1VwZkZabnZPL3pqNUo4cktDMStQd252Ym14Ykg0bnd5clhWWTQ3Y3AxaG12MXJHMmJiLzhzam5vMWtOZjdFdGN4VEJOL2I0OThTOUswNXNtV2lGeExHY2p6cHhlTWt5VE5tdXViWE9DeTQ0b1pjZTJ3MGVMc3lrdlBQOHlaL2QzdVhSdUQyRXJiR3RmN3RKeUVkSXIyb3h2MDdLaVpKdm5iTEtDNCtNMTYvV2F6U2FqckdxVWtoNm1vUkt5NjdlQ1hrTXltVXlaVGljSUtWSENXMjZsOEFhWlMrZlBNaG42akwwMFRkQ0pRQ2svMVBSVnJRMU9SdHR1bUZyQXA0dVNraUtNZWVNL2IzSU15eG8rLzZYbjJOdGI4c1NqRHlDY3VWZjhhYnZuM29rVDBvNm12UW1pSEJHRjZEWVhoNU9hYjc1OWk4Lzgxak1zZDAraHRXWlRsTmhreUUvK3VaOWdPQnBRRzhzSG52d1FlN3Q3QVdjZmlNT3g4OTdVL01hdi94Lyt3OS82QmM0cHhabkZIR3FMZGNZajNjTFhwbVdmLytEUE13ZUhSMGRZNXpoNzlqUmErZE8vTmJnRUQ3Mi9yWS9ZUDdXSFVySjl3SjJRbU5wU1ZpVkZXYkxOQzc4dVdhOHB5OHE3a0hSQ2txVG9SSk1PQjB3bVk3UUs4Y1dOYkNyNG9SR0MybFIrTnhwV2kxMG1TUlJ5RllrZFJHdEdjVDFaakhYV2Y0K3g1bDEwbVFEeGJSM0haOFdudE8yWmQyUmtjUExyTHllYUMrTUVTUzVxQzd4b3hkL1cxdFpZWTNEVytwZmMrUWpzbGdNZ1BIdStLZTJWY0dqcGI3ODAwZDdvbEdoL0V5cHZobEhCUHExYWg1OE5PZ2NQeEpTdVpqblo0ZXora3VGQWg2QkpYMSs0NE1Cc0UzeGwwTFFGUkhVYjNlRWtMNzF5bFFmdXY4amVjbzZ6WHVTU2x5VjVWWkZsSlZtZXM5cDRWZUJtdmZIVm5KQmVVYW9WV3ZrZVd5VkRscnNqcnowSnJWdXFmTVdYRERRcVNSaUcxVmlTK2hLOXJDeXZYYm5DUSsrN3lONWlHcklDNlNKZHdoQzRPU1J0NzRmUmo1bjNhbEVSaFkxNDZiTU16L08yY0h6dUMxL20wbjBYZWZ6aCs4QldIV0RPOWNOMFk4dld0MHhTYzUwV29pbmZMWXFyYjkvaGMxOTRqc1ZpbDBRcnRubEJaZ1dmK3RSUE1wdE5xT3FhaHg1NWxMUG56M1pKOUUyMVFYZVlmTzQzUHNjdi84eGY1NHlTbkp0UHdWaHFJZm9nVXlIUjB2a1F4U1lPYTV2bkhCMGZzN2ZjWXo0ZWRlUVcwY0VnbkhQY09UaWtxR29XaXgyT05odTJXY2xtdXlYTE1vcWliRjlFclpTL2VSTEZZalpCSnlsYUo1N0lZbDFrbVl6d3llMjk2UDlKV1JaSXFkQXlpU1M3OFljZEJZL0VFVll0OGxxMDdIcEx4eEZ3Y1RsTkYwUFdqMVFLLzF5NmFJL2U5Zkl0N0NLNjZwdU1RYThzczFoclFsa2VibkZiQmFHTmFDckxJS24ydjZaUytCdGFLWWJEMUsrTVVrMmlFaitnMGhJZDZMUk9kRkhaSWxha09kTStJTTFBVUFwZit1L3VMVG05bkpFcTNjcDhtd0NWTnJHcERhendZQkNEOE1hZHFtWmJWR1I1eGF1dlhXVTRHcEZkdWNicStKZ3MyMklxQTFLMjRwWWswU2lwVVZveW55K1F3c2V6S3dGS3FiYUVId3hTaG9QZzVrdjhJYWFsYXFmYS9tQXl2blYwY0hpMDV1cWJiL1BJNVFlWlQ0ZWRLMVA0dFcvRDlaZXQxVkpHY3c4UldjQ2JoOGo3SDNxbVdPRXdTSTdYSlYvODB2TTg5UEQ3dVB6QStSQVhUbmRwaElPL0hmUUZqRmZuM1lqRUZsR1ZhRjJud0xaQzgvWFhydkhGWjcvT3pzNHVhYUxKeTRxc2hyL3dxVC9QcWIwbHhscnVmLy9EM1ArKzk3WFZIeWRIQ3c2ZS8rS1grS2QvNldmWXFRMlhkdWFrdFovN0lNTmwxU1o4NDhOQkJRcHJhKzdjdVl2VWluTm56NUtxeEhQWWFrTloxNVJsUlZsVXJMY1o2ODJhOVhxTGxCS3BibmhVY2FyUlNqR2Z6MGtUVFZuV0dGT1RKbjZ0T0U1VGxGWnM4b0lzejl1OGRObjIzS0luUVFWQlZSdnFxaUJKQnlpbFdxSnJuRERiTjlOMDdVdWZGdVFUQWFyYWVQdW9FNGllYXU0RThhYjVPa1E4cXJYaGdJaGdKTTc2VzlzNnJLbjkyc3JZZHZMYnVlbE1SMzV4amxUNTZYSjdvMm1QeTBxVjMxZ29KZEZDdE5Qdzd0eUpPMm9UMkhZaTJ2L0x0dnVLWjVBQ1I2bzFaL2FXN0MwWFNPMERMMFNJdFc1MjZNWTRqQW5SV1VWQmxoVnN0MW13NFc0cDhtWUg3dGRqV2lma1JVbGQxeVJKd25DNGl3cWNmcVVsaVVwSXRXUXdISkNtL2daUFUyL0wxbHA2eUtjVXJUSlRoZ3ZBTlI0VGEwS2JGOXg4MGlQRTc5NDU1SzFyMTNuOHNjdU1SN29WV3duck9qdDBHeDBtbytRNkZRV1p4SHI5YUZFVFpkODZJYm03eXZuc0Y1L2x5UTgreGdNWDkzSGg1bytIcmIyRXZCTWdiZGR5RzVyRWFka08xNzIzUm1EUnZQRGk2M3psYTYrd3U3dUxsb3E4cWpqY2x2ejBUMytLMDZkM01kWnk0Yjc3ZWVEOUQ3WjZpMGhZMnJvdVgzN3VxM3o2TC80VjVwc3RsM2JtREFNaDJZck9nTjk4R3RJNXRKQ1NMQys0ZnZNV3c5RVlJUlRYcnIvckJ5NUZUbFdXS0tYUXFXYVFETkZhTVptT21jOW5hS2xRMml2K2tzUVA0alpidjJhWmpBWU1oelBHd3dGS0tySXM1ODdoa1E5ejZFa3p3MjJEalNidGtHVmJwRklNaDZNK01rbUl3SzhQTjYyTXB0NDRiTzFMdmJxdUtPdWFxcTZweTVxcU10U21vaElTS3pxM1Z6U0RqL2I3cmx1RE5vTTFVM1dRU21QYXI2VVZuamgvQ3lpQjV3K21QbHN1MWY2ejhVeDdqWkwrWWZaOE90RUtsNXJQdzdVSGpZdm9DKzY5T1I2OTlrM1NXeXFFRW5NK21YSjZiNWZKWkFUV1VWUWwyM1ZHV1ZSa1d5OVZYVzIyWkhsT1dlUVk2MUJTb3hORmt2cEVIYVVVNDhtYytXd2VFTnBleHBxa1BnOXZFRjdzUkNlazJ2KzNXaWxVcTF1MFBZTlNSd1F5TkcxMEdMdjV6OFJHTjJkN0NFcWNrTnk1ZThpN3QyN3h4Qk9YR1NhNmc4cTBaMk5RWEZyVDZmMWpCMnZrNW14bUc2NFgrTm9SVXU4ZTVYenVTMS9oSTkveEJPZjI1dDRDSEFSaXhya1dKUjVQM1pzSWRSZTFBRzI4WlpOTEdWb0dJU1JsNVhqK3BWZDU1YlUzMmQ4L2pjQlRoUTlYR1ovNlMzK1I4eGYyTWJYbDNNVUxQSHo1TWtyWUhnNUt5TTdnZHZXMUsvekRuL2xaQmpkdThNRCtMdVBHQzlHb1FNUDNheDFZWTFHbVFqLzd0VmZJaXBMYU9YU3k5dFpMcGRCYU1aMU9TTlNjSk5Ha0FlT2NhRi9PTmV1VmhndVhaeVhINnpWS0tZYURKcGdoSWE4cWpvK1BxQ29mQk5IVFk3dElzQkllQTJNTVdWNHlHcVlvclhzdmdaTitnbHdibjdCYVYvNEZyOHFLb2lyRHkra2YvR2FZT0VnVFJyT2huMk1veWFxc1diOXoyNk9lbmNJWmk3RmhpbTdxUUtFTnQ3anJtMlhDSllTU3dwZXNvUmRORTkrUGErMGhrVkk2YjRWdG1nN25vdXJHaG9mRVJwdUt5T012WXBHUGpOeHJ0dmVnK3BXVGpJUlB2cDN5MzRlbE5uWGcyVlY4N2VWdnNONXVLZkxTRTJXVTk5YzNMM2VpRllPQkQ3eFVVcUswSmtrRXFVNFlEc0pLTFBId3l5U1U5a3FLaUsvZjliUDBuTy9tUGVnSUlyd1EvWHpvMkpRcDR0KzBKWTNpK3MxYnJJNVhmT0N4UjFBcXpFWGFsYUxybUEzQ2I2cm81VU9JYnJmaStuYjJFMDU5SEhCd25QR0ZMei9QMHg5OWl0UExDVUtZRGk3ZlNNUmRIRE1mSTI0a1JnaHFZL3k2dWpRVVZjVW15OGx5enlIY1pHdnliYzVtdXlVdmF4YUxKWW1XNUZYTjRUYm5wLzdDVDNIZmZSY3cxcko3YXA5SEhuODhNQTJhRjduQjBmZzUyY0dOZC9uRm4vc0Z6RGUreWYyN08weGFLYnpBU2dmR2k1R01rQmpBT012RlUzUDA3dTVPRTVpT1VqTElFMVd3YkFaZW1vZ21tTkZpeDFtb2pXVzEyV0N0UXl0Rm1xYWt3eUhHR083ZXZVdFIxcUhQbFVISUlpTDlmYXhYbHhTRlgvR01SbU9NcVNtTGlycjJLNld5ckNpcUNsTlhXR2ZEZEZlUURsTFNOR1U4R1pKb2paWmVLWVlRYlY1cWsrZG5yU1VKTDQwcE0rb3lhM1hrVFVtdmdoNStNUERDajZGTzBGcUZIbHo1QVp2c3VFQTk5Vjd6Y2xvWEwvSGFQUzg5UjVycnFjRzZrQTdSUTNHMWNFNlpoSGJEVXBvYVUxdXEybURxbXFJcXFjc0thNjJmdVdpRjFwbzZTVEJGaGRhS3hXSUh0UnNzTnNxcjNMVFNKSWtNNWgydm8waFNIUTczeGpEekhtNWthNEovcHhtZ2RRR2RNZUdvN3psMGZaZG1DTkNLUys0T0ZpVENEZGtndEJSdnYvTXVlWjd4eU9VSFNRSnowb1dicjExTk5xMmE3UlNCcnZmM2RoTTVyK3pydE1FT0c0SThKUWZIR2M4OC8zVSs5dlNIMlprT1E4VXBlaFdGMTliN05yV29hOHFpcGlnTE50dWM5V2JEMGRHRzFXWk5WZFZlSDZza1Fpa1M3U1hBUHIxbndtQTg1ZUR1SWFOUlNsbFpqalk1UC9ySkgrZnk1WWVwNjRycGZNNFRUMzdJKzIrYTI3OFpVb1lmeVBIZEl6NzljMytielZlZjUrTHVqSVhTU0Z2N1NoZlp6N3NVRW1kTDloY0R6c3hTOU5uVGV5R3JUSndvaDd2cHBzTWltbHozSU55eHhwSmxPWFZ0Z2lNTWhzTWhTTUhSOFRIYkxHdmx0VEdJeElZaGs2K3NEVlZ0cUtxYXN2SVRZK01zMWppVUVQNERDekhOZzBIS2REcEdTNGxVaXUxMjYxbHV3eUZ4Wkhic3JITXUvdnZEb1NCZ3FCVTZVUXliTWoxTTBiVlNBU3ZtVjQ3ZVcyVERZS2k1NVN4eFNKNklOUHZpaEFpajBTTUkxNjBnWFNONWxib2JlRFpESyt2QzkrKzNBVVZRTnRhVlB3anJ5bTlDcFBJcnNDVHhVdTNGZEJLRVVKcFVhdzloa2FJVlJ5VkpVNjVyVWltUjJxL1JSSXg1ajRZSHpXYWxZM1YxVkdicFJFOUwzQjVza2V6NG5vZ1UwYjFvUWdUVGxwTnRhOVA5S3FMMnhmOGNMSkszcjkvRUFwY3ZQNEF6ZGZoNm95UythQUxiWnZXMVIwelhsRHNiVXFLUldOR3RkdjMzcmJFSWJ0dzY0T3V2WE9HREgzb1NwU1IzanJZVVJVbFJCbXBSbnJOYXI5bUVqWlpFb0hUNGJFTVFxVktTSkVuWlA3WFh5b0Jsb0ZZcnJSa05Va2JqQ1hsbGVlbmxWNW5QNXBTVjRYQ2Q4eWQrK0UveDVBY2Z4VnJEZExIZ3lhYytSSkxxcms1MC9TU285Y0V4Ly9odi9WMXVmdmF6WEp5TTJFc1VzamJZV0hQYW1GdXNyNXJHU25CcE9VT2FESzJidFZxei9vZ0t6Wk5Ca1FKQlpReFpXV0JLRXdZenpacFhzczR5OHFJSVdDaU5OWmE2OWhMaXFxNnBxcHFxcnFpcm10b1l2d3BTSW13SkVwWTdPLzdCVnNwcm9sVTN2Vzl1R21zTlIwZkh6S1pqMGxUVDBoNWNmNUFZVTNjN01ZMWdPa3g1NVA3ekxhTk9PaHRKZ0IyQzJqdlVvb2REdUk2UjMzd1NyaGQwSWsrd0QyMjNqZ3pWalhFbWVCTU10VFZVZFVWVlZkU2hoYW1xS3VDdEhFcjZHenhKTklNMFpUeElTWFNDVmo0b2dzQzFINlFKazlHSTRYQkFVZGFzajFkSURPZlBuR0l5SHJRM2VMTVdhemNDelhxVjdxQnFNV0pSRW5Lc3B4REM4eGFhWkoySXBOSTdLMlFFVGVsUmpDTXFqWWlqbWwzSERuVHR4TjRodFJjZ1gzdm5Kc1BCZ0hObnoyQnNGVGxGSTYyRmFFemVjWkt4YkExandubFNvUlBPbytKS1MxWVViSXVTcXF6WlpobEg2eldiYmNiUjhSb0wvT1puUHUrVmNtSFdvV1U0VElPSWFMUzc1OXNsS2JER01KdU4yME4ya0NnR2VvQk9GVnBKa29aMUlEdFoxdDNqbkM4Kzh5Vm04em5Xd2EyN3gvenhQLzNEUFBYVWt4aG5HSTRuUHJ3ajFjUkJ0UzR5VkdTckRiLzg2Vi9rclYvL2RjNU5ocHhLRTVUcDBwbHMrUG1iVU4xWVc1TUl5MUJKTkJhSlE3ZDVZMFFXVFJkQktwd3Y4NHV5OVBwNlo1RmFlMGxoc0VjV2VVbVdGNVJaUlZHWDRVV3Z3UmcvTFc1MTE1clJ5QThTbFF5dXNzQ2hkODJEMnJRRnpScU5wcC8yVXVQdFpzMXlaNmRGaERjYWVmK3dtbEFlZGNaWlFSeHEyWWlkUkNUWWFCSnRiTVFubHlkUTFpTGdtdjFENVBma3NpY1B0bUhhYTYzeFBiaTFWRldORFJWT1ZmdER6em5ucC94aG5wSWtpdkhNVTNLVTlCc0FHV2kxa2ZtOHpRSncxakVaRHptOXYyUTZHaUlGSEI1dFdSMnZtWXhIbkRtMUpORWk4UGhGckFOcy9mZXRyNktYY0JiZGxCRWdxTzJRNC9SYlIwL0kxSmlSbW9mTjh3TTdpWGVuTVdtcVFOZFdBNTB6MVBYRU1wVnh2SFB6WFdhekthZjNsajVTekt2L2NZMUpLbmp3SFpLaXJ2M2hXaHVLeWxkT2VWR3d6UXF5ckdTN3pjaXlqQ3pMY2M2M3EwblE5cmN0YnpyZzlPa2hFdEdhZlZMdGRRaUQxRFAvZllKUkVyWTNLVGR2M0dRMkc3T3ptSWZLMlhqSGVVeGFEMVpjSzhCWXdiVjNEL25zRjU1bHNkakJJYmgrNXk1LzlFLytLVDcyOU1jd1ZZbE9FNTU4NnNNTUJra0VabzEvUGhaYld2Nzl2L3AzUFA5Zi9pdm54MlBPakVhb2NFQzZjRGpMWUtZU1ZxQVY3SjA3eGR6Q25iZmZiZzllTFdpc2k2cXpTb2FYY1ZNVTVOdUNvcXlvaktHdUxWVmRlaVZYN2w5MGEycWtrcjc4MFpwaG1qSWVqOEtnS1BqTHBhUy9zM2dQRUs4UVBVOTMzNzRxV1c4M2xFWEpjcmtiL000aVVnclFzZ0I2cVRkeHpvRHJiZ1pPZ0RVNndYTG81Mk9rZDFDQk5UT0UydmhoWVZVYmYzdlh2anl2YXI4R3hMb3dNMGpSS3BoTVJpbUxkT29WamNGSjF1b3FnZ3V5UTQ2NTFpUmttOWJHK2lwa01obHhlbStQYVJPT2dlUDI3V09PVjJ0bWt3bW5kdWJlNU5xTHhvcWozaVRkaUYxRW9GTWJNUU9DTU1yU0R0RmtUQ0FLdDRvVW91OUtkSTQ0T014L0ZyNjBrRUV3MVNqZlpQUmlOQTYrN2s4VXhzSzFkMjV5OXN4NVpyTWhsYkZVdGZlRFpIbkJOc3U5a3k4dk9GNnQyR1lGbTAxR2tSZCtpeUs5LzE0cDc4blhvZWYyTHRjcFdpaTBnalJJb2RPR1RKVG9vQ0tVWWRiaktWVWl2QjhpL0d5YUErckdPemZZbWMrWXowYTRFTlRSRGo5RVAxZXpVVWErY2VNMnYvbUY1MWd1bG1pbHVYMjQ0dmYvd0EveXNZOStDR2NxaEZJODhkU0hHWTJDc3k5U3NpSjhQTGt6RmYvdFAvMVgvdGUvL05lY1NoUE9UTWRnNm5Eaml4UHNTWWNVanVVazVlbkxGL244cTIraWhXdG5Mcm9TSUl6RDFpVkZWVkdVL3RUMG9wNmN1dllwTERwSmZCa2FidlRGUENYUnZyUVJ6USs1bWJVMkwzQ1BFQ3UrRFVldktiaWxIOFM0Um9ubUorYkh4eXVra3V3dGQxcHZlL3RxQzljL0pVVThTWmNuaHBmUjEyS0RNQ0pzSTV5VEdCdmFGZU5mYW1lQ0JxS3VNSlhmRUlpd2sxYUp2OEZINlJBMThkTjBuWGc2anBLKy9LeU5JZHR1bUU2bVlkOHRXbXBzL015TFNIUGduWmdOdE5SVGV4ZlRLYWQyWm93SDNqdnVLd0hCelR0M3lmS1MvYjA5SnFPMFJiZTUyS1VnNkdkK3RkMlM2RkN3b290Y2FzVkVJZ29CalpLRW5YTW5VcDdwSjBuSDNNa3c4M0dpNGRuN240Y1VBaWU5VE5oWUwzMjJOUlJWU1ZFYmpvNVdaSG5OM2MxYnJGYUhiTGU1aDhBWUY5cEdYNHBMN1RjV2czVEVJQjJGMkM4Zko1WW1taVQxTE1FazljQ1NKTXgrdkE4Z1hIeGhNOVBNQmp6N3Z4bE11alovd1FuLzJWb3BxVXJEdXpkdXM5eGJNSjBPVzlOVVkyMXRRTFJOcTJQeHBOODMzcm5ONTcvOEFzdmRQVktsT054cytNNlBmNXp2Kzk3dndUbExWVlY4K09uZncydzJqZEtaWFB1Wk8rY0JwNy8rMy84SHYvS0x2OFJlbW5CaE1rYUcxQ0gvZDhvZWtFWTVSeW9OVHo1OGtVUldhUHlnV3dsL2lPc3ZmK1ZyS09GUHZDVFZTT1gzdWQ0ZE5VSW51azAvdGRhRWw2REw4V3VtMmMxcXEvV3l0N2FYU0UxM1FoQVo0N2NROTlyZWpZR2pvd09tazZuM09vdXVMSThqd0p1ZGQwUCs5ZFZFYzFBMGUxS0JzUTVqdlBHb3F2ekxYVlVWVlZuNndac3pTQ0g5aER3TUI5TWtZVFFha2lpLysvWTN1SGU0MGNwbWlRaTdEdXNNWlZtUlp6bnorUndoVkMvOG9mR25jNkpKaVJIclFzRE96cFRkMlpSaE0rdHdGcHpDV01HTmQyOWpnZk5uOWtsVVFGTUxHZXk5SjkyQzdnUzhPY29ha3JGWm9VdUJqdGVMemVTN29UeGo2Ym5ZR3VDSndHc3NtdXZGV0c4SDlySndyeUwwMHZDS2JaYXpXVy9aWkZ2S3NtclBiS1U5TVVkckx3dlhTakpJVW1ianNkY1lCQmFBWDBjbmJEY2Jsc3NkVHUwdFBOVXFjQ3VrQ0JtUUlZNE1HMmN3eWg1OHd6cmI5U2RSVkhxekpyUk43cUFRYkZjNWh3ZUhuRG05ejNDb3ZLUTdwaXZad0F1MFFUS1B3QW5GMWV1MytlMW52OHJ1OGhSU1NvN1dXeDc5NEpQODRBOTh2NjhOcE9ieEo1OWt1YlBvVmN1dXBVZjR0dTYzZnVNMytUZC80K2ZaYzVJTDh3a0RhL28vNFRCclFZQzBqZ0dHeTJlWFRFU05jTUVoR3pFaTlObjlmWVpwU3Bwb2xJSlVKMTdKcGZ3VTFnR3I3WmJOTnV1N2wxcFhuT3lVY1lLdUJJOXo1dS81WDhUUTd4RmhYTHZ2enJLQ0lpOVk3dXdndlpjWEc3NVpmMUtIZ1lqd1pXTlEydm8rMEJoTVZaSFhGWFZ0cWF1eVhTYzJ3NVFreER4TmhrUDBkTnlKbXFUcUVvUkZaMkdtclVwY2o2WGZyWkZvN2MvcmJRWVc1ck9kVnFBa1haT0lhYUxQMFVZRFJIOVlLUVhMMll6ZHhReXRRbjl0VFJocVNZcXk1dWF0dTZTRGxMTjdPOGhtNXVGY0JEWStjWnM3NGJjNHJsT2swZENmall5bStCRllKQkxpTkx0dEljTitLRkNoYStQQ0ZxY0lGbDMvWW1kWlRyYk55YkxNNno4Y0tKMkVLa21IejlnTHlIYm1jei8wVkxJRGZhZ2tiQy84Tkgyb05XbXFBa0JVaGZtUDVLM3I3eklmNzNEaC9CbWM5VlpuWEIwT244cGJxazg0Y2x5QXhEYlRkTC9sa3AwcnZmVzZOVnViWm5ncFdSMTVhTWo1QzJkSWRDakhiWjhWMFlhQk5KV2xWRnk1ZHBObm4zdVozWjA5cEZZY0hLMjU3K0hML05BUC9iRWdKaEk4OHZoajdKOCtGWGxSSW1xTDhPRWhYL255cy96enYvelgySE9DUzdNSmc5NWgzM0VSYmZDU0pOUThkR2FYcGFaRjNTa2hBakhaUDNINjBubi96ZWhRa2pWVnVYV080L1dHdytPVm4wNDMrMTdYVjVweElnRllmTXRBN0xqL2x4MkhQNTVzaGwvVzZ6V0pUbGd1NXhFTDNXQ2RDeSszRFpzRkQvN3cvWGdGMWlHbERDdVpCSjFvUnVNaFd2dVVHR01keDhjckp1TVJvOGtJR1JHRld1TlF1K0d6UFN5WXY4Vk42K1pxSXNWY25MZm40UGg0UlRwSUdZMkhBZjJsMnBCSEViSElYS0MrZU9lY1laZ29kcGRMWnRPeDcrOXRJeUh6akdvaEJKdXM0TjFiZDFqc0xOaGRUTDMwMkxxSWZXZjdvU0ZOcXlUNnJNQ3VUeFNkMWtCNEpqNUdZR3J2WFNqcmlxcXN5Y0t0WGVRNWVWbXlYVy9KaTdBQ0Z0SWJ1Z0lUVUd0djhra1N6N2J6ZHUyZ0xRbER6elJOZkZKd0VqUUxnUjhwWlNmamRzMWNJancyM3FYcWJjNTVibm5qemV2czdpMDVkM2JmTS90c1JHKzJnVExqb3BCd1IvVHBSTDl6dGlNb1dSR29WelpZZjhQOHc4TGg0WXJOWnNQRkMyZDhGaUJlazlHZzlPT3d1RTd0b1hudHluVmUrUG8zMk4wN2haS0tnODJhK3k0L3dvOTk0aytqbEtTcWF4NSs1QkhPWFRqZnJvb2I0R2luaFJLOCtNSUwvSk5QZllwNXR1SCsrWUpCVFBvOWtlSmtxUm00bXZlZjJXR1pDbjg0aG45WFNrOTFrb0hCcVQzWHJFc21kUUx5c3VMdXdWR2c3aEx0cXZ0Y3FaYm0wbVBidVM3MksxclBFQ20xdWsyR0RKOTlVRTFady9IUk1VcHBuUFBybVdhcVc5ZCsveXVWSk5WSm1FbG9KdU5SQ0hqVUVmTlB0SnFENW9iTmk0TGo0dzJMK2NKUFYyMWZDQ3lpUGZVOTdyMFkyeGtyMUlJVTA0VWQ2OTJESTJiVEdUcFJOTlYxVzk0TFRueGV2bFVZcGltN3l5V0w2UWdwN0QwSlBVTDY4dS9vZU12QjBSSDcrNmQ4dngvb21pSXFXMFhZNVFuWDFvQnR6bDVERDY1cTU0ZVhWVVZXbE9SRjZZMWMyWVlzeXltejBxOXlyZlhiaXNUZjNHbmkzWnZPR1lhakliUFpOR3h6ZkxhZUNpOTNvemxJVmZkeU55NVBKV1FFNTNNOXJKcHdIWGtrSnZnMkEyUS9GQlZzMWpsWDNuaUw4K2ZQYy9iMEhxYXVJeG0zQ1BxQ2JrOXUyN1d0OHlZaDBabTdYWlJ3M0QzYXRtVTdhdUU1bG5mdUhGS1dKZWN2bmtYS1FPMFJIVDI1QVprMDh5WXZ3RWw0NmNyYmZQV2wxOWpkOVRmNzBYckQ2VXNQOEdNLytpT2txY1pZeC9zZWZKRDczbmVmMy95RU03OUpCeFpLWWEzbHh0VTMrYVcvK3JPa2QyNXhhYkZrSWdLeHF3MFpqZDVDWnhHdTVIMzdTL1lIMnMvM2hNSmEyVnFQWldOOEEzU2pZUWMvdERvOE9tYWI1MGlobTBDNlRzL2RDOGpzZ3krN1JOVEFkSlB4eVNpb25aOGgxTVppYXhNSVA2RVBMMnRxVTdjN2NLV05uMGxvelhnMFlEN1RxRkQ2S1JIWGFxNTNtb3NlV0tIanJoMXZOdVRiak4zbERscnJlTDkzRHo0NzdzTzczOW8ydGJYenQzZlRpcnEySEs5V0xIYm1ubHZZSEc0blFkMGRLWVRSS09YVWNzWjBPUENiRXVkNlN4TFJ6amtFZDQvV2JMT2M4K2RPazJyWi9yeGFoYUh6dlg5ZFcyeHRNWFZOYVF4WmtWTldGWGxla2VjNTJ5eW5MRXVNOFRSZEZTeTVLa2kvQjhNaHMvSFV6em1VYUczRldnbTBWcXhXVzVTQTgyZlBvQkxsWFgxU2VrKzVlSzlzNUpDMjAvZzJJZ1ZoN0tjVVFnVlZvT3dPaGJZRU4rM1BZWjBWdlBIR05lNi8veUtubHZOZ3dPcVNpanNuSm0zWjNvQm12ZDBYWkJ2R2FkdlpsTGYraStaeGFnOHBpK0RPM1NPd2hndm5UaU5WZzFBVGtZZ3IyaVlKTURpc1RQbm1temQ1NGFYWFdPN3NvS1Jnc3kxWm5yL0FKMy84RStoVVl5MmNPWCtPOXovMGNDUmpqcDQvNFZlK2QyL2M1QmYvNXMvanJyM0ZoZGtPVXltOCs3TTVjTUtoMzVLSU1GeGNMamc5R25qUmxKQmVMaXhsRzlzbW8xbVFsZ0lxNXpoZStWUlZYeGFxUHQxR2lCUGwvWW1FbWhES1VWdi84Rm5yL042N3FxbXFraXBrOHJsQWZtbGt4bG9yeHNNUmF1SWZRQzkwa1oyTTF3VVNqWWl6MVVURWt1dDRhdDNMS2R0d1V1c2t4MGRyckRYczdlNzVsWTQ3R2REMU8yUnhuZWl2UkR2RDhGL1hkcHRUVnFYWEp2UVNobXlYSCtpNlkzSTJHYkcvdThOb2tJWXl0WkhFdXFpaGtLQjhGM0R6OWdGbFpUaTF1MHRlVlJ4dFNyTENrQmNGUlZHU2JmMHdMUzhMYW1PRFYwRzFha0ZmYnZ0dHpYUThRczNHWHVvYnltNFBDUEdyTXFVMXFkTG93RkZVMHVPcmpiVzg5YzROeHNPVTkxMDYyNThUdUJNMnVNaDgwK3J2WFRRWGJtLzRXRTR0aUwxaDdRRWQvdHhhV0swTHZ2bjY2enh5K1VGMkZoTXZpNDdxdDk3SU9XSXR5bGIwMURBTjR0eUgyRUo4WWl1RDVNNnRPd2dsT0h2K1RDdUVFc0pyT2JxWm9xU0pMQktobjMvaitpMmVlZTVGZG5iOHFtK2JGNlN6SFQ3NVp6N0JhSlJpSFN4MkZ6eDgrWEpFTVhaZHFMYjBCOHpoN1R2OGc1LzcyOXo2OGpPY0g0MVlhQTIyYXNOcG0vZXkrY3lVTTV5WkRiazBHNk9ENVp4b050V3NzdjI3TEJGSWRKYVhIS3hXMU1iaVVDMzBzb1ZjaEcvY1dYOXFlckdGajJXdXE2RHFDejBqMGo5OFhzbW5xTXFTMlh4R29qVkNnbFk2ckF5YjBmTUpvRlliNlJYM3NzM05hTHQvMnhJOVFOQWJXNGQ3eHhqTHJUdDNHUStITEJhejl1bHl2NnNYLzU1MFUwNFlQQkhDOS90S0tSYnplVytRUndRdGRjNjNKVHVMS2JzN0MwYUpibThNcjlOVzFOWlFWcVlGcUJSRnlYYTdaYlh5SUV0akhjNWVhUldYTXV5NGZacVNaaktkTXBWVGYyTnJTYUkxWlZVeFVJTEZZazZpWk5SbiswTlc5cmpzM1ZDMmNhazFoMW1lVjd6KzFqWDJ6NXppM1A0T3pwazIyYmFOVHVpQk5VNEVwZ2ZjZUU4SkpPS3hyNHpQak5ZblQ1QU1Xd3VINnkydnYvRVdqejl5bWNrNDhRWXVCRklFUTYvdzVoYUpiS1dJUXNSUFF6aElHa1pDbUVHMTJjNnU4UUdHY3RyQXJWdTNtYzRtN080dTJySnBlQk1BQUFCNlNVUkJWR3JRV0V0dWFzcThKaTlyOHJMMHJzb2lKOHNLWDJWdGM0NDJHY3VkUGRJa1liWFp3R0RJVC96WkgyYzhHV090WXpLZjhZRVBQb0dXVWFLVDYrekI0Tmdjci9qbGYvUkx2UDI1ejNNcVRWZ2tHa3pkUG91TjBhZHhWK0ljZStPRUI1WXpVbE9obkxkaWVSZWxoK0EwMUdVYm9LNEkrSCs0YTlNQXp3RFAyUUFBQUFCSlJVNUVya0pnZ2c9PSIgYWx0PSJDdXBvbGEiIHdpZHRoPSI0MiIgaGVpZ2h0PSI0MiI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iY3VwLXR4dCI+CiAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJjdXAtdGl0bGUiPkV4cGxvcmUgaW4gQ3Vwb2xhPC9zcGFuPgogICAgICAgICAgICA8c3BhbiBjbGFzcz0iY3VwLXN1YiI+U1FMIHNoZWxsLCBwaXZvdCB0YWJsZXMgJmFtcDsgY2hhcnRzPC9zcGFuPgogICAgICAgICAgPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImN1cC1nbyI+PHN2ZyBjbGFzcz0iaWMiIHZpZXdCb3g9IjAgMCAyNTYgMjU2IiBmaWxsPSJjdXJyZW50Q29sb3IiIGFyaWEtaGlkZGVuPSJ0cnVlIj48cGF0aCBkPSJNMjIxLjY2LDEzMy42NmwtNzIsNzJhOCw4LDAsMCwxLTExLjMyLTExLjMyTDE5Ni42OSwxMzZINDBhOCw4LDAsMCwxLDAtMTZIMTk2LjY5TDEzOC4zNCw2MS42NmE4LDgsMCwwLDEsMTEuMzItMTEuMzJsNzIsNzJBOCw4LDAsMCwxLDIyMS42NiwxMzMuNjZaIi8+PC9zdmc+PC9zcGFuPgogICAgICAgIDwvYT4KICAgICAgICA8YSBjbGFzcz0iY3VwLWFpcm93IiBpZD0iY3Vwb2xhLWFpLWN0YSIgaHJlZj0iIyIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPgogICAgICAgICAgPHN2ZyBjbGFzcz0iaWMiIHZpZXdCb3g9IjAgMCAyNTYgMjU2IiBmaWxsPSJjdXJyZW50Q29sb3IiIGFyaWEtaGlkZGVuPSJ0cnVlIj48cGF0aCBkPSJNMjA4LDE0NGExNS43OCwxNS43OCwwLDAsMS0xMC40MiwxNC45NGwtNTEuNjUsMTktMTksNTEuNjFhMTUuOTIsMTUuOTIsMCwwLDEtMjkuODgsMEw3OCwxNzhsLTUxLjYyLTE5YTE1LjkyLDE1LjkyLDAsMCwxLDAtMjkuODhMNzgsMTEwLjA1bDE5LTUxLjYxYTE1LjkyLDE1LjkyLDAsMCwxLDI5Ljg4LDBsMTksNTEuNjUsNTEuNjEsMTlBMTUuNzgsMTUuNzgsMCwwLDEsMjA4LDE0NFoiIG9wYWNpdHk9IjAuMiIvPjxwYXRoIGQ9Ik0xOTcuNTgsMTI5LjA2LDE0NiwxMTBsLTE5LTUxLjYyYTE1LjkyLDE1LjkyLDAsMCwwLTI5Ljg4LDBMNzgsMTEwLDI2LjQyLDEyOWExNS45MiwxNS45MiwwLDAsMCwwLDI5Ljg4TDc4LDE3OGwxOSw1MS42MmExNS45MiwxNS45MiwwLDAsMCwyOS44OCwwTDE0NiwxNzhsNTEuNjItMTlhMTUuOTIsMTUuOTIsMCwwLDAsMC0yOS44OFpNMTM3LDE2NGExNS45LDE1LjksMCwwLDAtOS40NCw5LjQ1TDExMiwyMTUuNjYsOTYuNDksMTczLjRBMTUuOSwxNS45LDAsMCwwLDg3LDE2NEw0NC43OSwxNDguNSw4NywxMzNhMTUuOSwxNS45LDAsMCwwLDkuNDUtOS40NEwxMTIsODEuMzRsMTUuNTEsNDIuMjJBMTUuOSwxNS45LDAsMCwwLDEzNywxMzNsNDIuMjIsMTUuNVoiLz48L3N2Zz4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJhaXRleHQiPlByZWZlciBwbGFpbiBFbmdsaXNoPyBBc2sgQUkgYWJvdXQgdGhpcyBkYXRhPC9zcGFuPgogICAgICAgICAgPHN2ZyBjbGFzcz0iaWMgY3VwLWFyciIgdmlld0JveD0iMCAwIDI1NiAyNTYiIGZpbGw9ImN1cnJlbnRDb2xvciIgYXJpYS1oaWRkZW49InRydWUiPjxwYXRoIGQ9Ik0yMjEuNjYsMTMzLjY2bC03Miw3MmE4LDgsMCwwLDEtMTEuMzItMTEuMzJMMTk2LjY5LDEzNkg0MGE4LDgsMCwwLDEsMC0xNkgxOTYuNjlMMTM4LjM0LDYxLjY2YTgsOCwwLDAsMSwxMS4zMi0xMS4zMmw3Miw3MkE4LDgsMCwwLDEsMjIxLjY2LDEzMy42NloiLz48L3N2Zz4KICAgICAgICA8L2E+CiAgICAgIDwvZGl2PgogICAgPC9kaXY+CiAgPC9kaXY+CgogIDxzZWN0aW9uIGlkPSJjb250ZW50cyI+CiAgICA8ZGl2IGNsYXNzPSJjb250ZW50cy1oZWFkIj4KICAgICAgPHAgY2xhc3M9ImxhYmVsIj5Db250ZW50czwvcD4KICAgICAgPGRpdiBjbGFzcz0iZmlsdGVyIiBpZD0iZmlsdGVyLXdyYXAiPgogICAgICAgIDxpbnB1dCBpZD0iZmlsdGVyIiB0eXBlPSJzZWFyY2giIHBsYWNlaG9sZGVyPSJGaWx0ZXIgc2NoZW1hcywgdGFibGVzLCBmdW5jdGlvbnPigKYiIGFyaWEtbGFiZWw9IkZpbHRlciBjb250ZW50cyIgYXV0b2NvbXBsZXRlPSJvZmYiPgogICAgICAgIDxzcGFuIGNsYXNzPSJjb3VudCIgaWQ9ImZpbHRlci1jb3VudCIgYXJpYS1saXZlPSJwb2xpdGUiPjwvc3Bhbj4KICAgICAgPC9kaXY+CiAgICAgIDxidXR0b24gY2xhc3M9ImJ1bGsiIGlkPSJleHBhbmQtYWxsIj5FeHBhbmQgYWxsPC9idXR0b24+CiAgICAgIDxidXR0b24gY2xhc3M9ImJ1bGsiIGlkPSJjb2xsYXBzZS1hbGwiPkNvbGxhcHNlIGFsbDwvYnV0dG9uPgogICAgPC9kaXY+CiAgICA8ZGl2IGlkPSJjYXQtc3ViIiBjbGFzcz0iY2F0LXN1YiI+PC9kaXY+CiAgICA8ZGl2IGlkPSJjYXRhbG9ncyI+PC9kaXY+CiAgPC9zZWN0aW9uPgoKICA8Zm9vdGVyIGlkPSJmb290ZXIiPjwvZm9vdGVyPgo8L2Rpdj4KCjxzY3JpcHQ+CmNvbnN0IElDT05TID0gewogICdpbXBsJzogJzxwYXRoIGQ9Ik0yMjMuNjgsNjYuMTUsMTM1LjY4LDE4aDBhMTUuODgsMTUuODgsMCwwLDAtMTUuMzYsMGwtODgsNDguMTdhMTYsMTYsMCwwLDAtOC4zMiwxNHY5NS42NGExNiwxNiwwLDAsMCw4LjMyLDE0bDg4LDQ4LjE3YTE1Ljg4LDE1Ljg4LDAsMCwwLDE1LjM2LDBsODgtNDguMTdhMTYsMTYsMCwwLDAsOC4zMi0xNFY4MC4xOEExNiwxNiwwLDAsMCwyMjMuNjgsNjYuMTVaTTEyOCwzMmgwbDgwLjM0LDQ0TDEyOCwxMjAsNDcuNjYsNzZaTTQwLDkwbDgwLDQzLjc4djg1Ljc5TDQwLDE3NS44MlptOTYsMTI5LjU3VjEzMy44MkwyMTYsOTB2ODUuNzhaIi8+JywKICAnY2F0YWxvZyc6ICc8cGF0aCBkPSJNMjE2LDgwYzAsMjYuNTEtMzkuNCw0OC04OCw0OFM0MCwxMDYuNTEsNDAsODBzMzkuNC00OCw4OC00OFMyMTYsNTMuNDksMjE2LDgwWiIgb3BhY2l0eT0iMC4yIi8+PHBhdGggZD0iTTEyOCwyNEM3NC4xNywyNCwzMiw0OC42LDMyLDgwdjk2YzAsMzEuNCw0Mi4xNyw1Niw5Niw1NnM5Ni0yNC42LDk2LTU2VjgwQzIyNCw0OC42LDE4MS44MywyNCwxMjgsMjRabTgwLDEwNGMwLDkuNjItNy44OCwxOS40My0yMS42MSwyNi45MkMxNzAuOTMsMTYzLjM1LDE1MC4xOSwxNjgsMTI4LDE2OHMtNDIuOTMtNC42NS01OC4zOS0xMy4wOEM1NS44OCwxNDcuNDMsNDgsMTM3LjYyLDQ4LDEyOFYxMTEuMzZjMTcuMDYsMTUsNDYuMjMsMjQuNjQsODAsMjQuNjRzNjIuOTQtOS42OCw4MC0yNC42NFpNNjkuNjEsNTMuMDhDODUuMDcsNDQuNjUsMTA1LjgxLDQwLDEyOCw0MHM0Mi45Myw0LjY1LDU4LjM5LDEzLjA4QzIwMC4xMiw2MC41NywyMDgsNzAuMzgsMjA4LDgwcy03Ljg4LDE5LjQzLTIxLjYxLDI2LjkyQzE3MC45MywxMTUuMzUsMTUwLjE5LDEyMCwxMjgsMTIwcy00Mi45My00LjY1LTU4LjM5LTEzLjA4QzU1Ljg4LDk5LjQzLDQ4LDg5LjYyLDQ4LDgwUzU1Ljg4LDYwLjU3LDY5LjYxLDUzLjA4Wk0xODYuMzksMjAyLjkyQzE3MC45MywyMTEuMzUsMTUwLjE5LDIxNiwxMjgsMjE2cy00Mi45My00LjY1LTU4LjM5LTEzLjA4QzU1Ljg4LDE5NS40Myw0OCwxODUuNjIsNDgsMTc2VjE1OS4zNmMxNy4wNiwxNSw0Ni4yMywyNC42NCw4MCwyNC42NHM2Mi45NC05LjY4LDgwLTI0LjY0VjE3NkMyMDgsMTg1LjYyLDIwMC4xMiwxOTUuNDMsMTg2LjM5LDIwMi45MloiLz4nLAogICdzY2hlbWEnOiAnPHBhdGggZD0iTTEyOCw4MEgzMlY1NmE4LDgsMCwwLDEsOC04SDkyLjY5YTgsOCwwLDAsMSw1LjY1LDIuMzRaIiBvcGFjaXR5PSIwLjIiLz48cGF0aCBkPSJNMjE2LDcySDEzMS4zMUwxMDQsNDQuNjlBMTUuODYsMTUuODYsMCwwLDAsOTIuNjksNDBINDBBMTYsMTYsMCwwLDAsMjQsNTZWMjAwLjYyQTE1LjQsMTUuNCwwLDAsMCwzOS4zOCwyMTZIMjE2Ljg5QTE1LjEzLDE1LjEzLDAsMCwwLDIzMiwyMDAuODlWODhBMTYsMTYsMCwwLDAsMjE2LDcyWk05Mi42OSw1NmwxNiwxNkg0MFY1NlpNMjE2LDIwMEg0MFY4OEgyMTZaIi8+JywKICAndGFibGUnOiAnPHBhdGggZD0iTTg4LDEwNHY5NkgzMlYxMDRaIiBvcGFjaXR5PSIwLjIiLz48cGF0aCBkPSJNMjI0LDQ4SDMyYTgsOCwwLDAsMC04LDhWMTkyYTE2LDE2LDAsMCwwLDE2LDE2SDIxNmExNiwxNiwwLDAsMCwxNi0xNlY1NkE4LDgsMCwwLDAsMjI0LDQ4Wk00MCwxMTJIODB2MzJINDBabTU2LDBIMjE2djMySDk2Wk0yMTYsNjRWOTZINDBWNjRaTTQwLDE2MEg4MHYzMkg0MFptMTc2LDMySDk2VjE2MEgyMTZ2MzJaIi8+JywKICAndmlldyc6ICc8cGF0aCBkPSJNMTI4LDU2QzQ4LDU2LDE2LDEyOCwxNiwxMjhzMzIsNzIsMTEyLDcyLDExMi03MiwxMTItNzJTMjA4LDU2LDEyOCw1NlptMCwxMTJhNDAsNDAsMCwxLDEsNDAtNDBBNDAsNDAsMCwwLDEsMTI4LDE2OFoiIG9wYWNpdHk9IjAuMiIvPjxwYXRoIGQ9Ik0yNDcuMzEsMTI0Ljc2Yy0uMzUtLjc5LTguODItMTkuNTgtMjcuNjUtMzguNDFDMTk0LjU3LDYxLjI2LDE2Mi44OCw0OCwxMjgsNDhTNjEuNDMsNjEuMjYsMzYuMzQsODYuMzVDMTcuNTEsMTA1LjE4LDksMTI0LDguNjksMTI0Ljc2YTgsOCwwLDAsMCwwLDYuNWMuMzUuNzksOC44MiwxOS41NywyNy42NSwzOC40QzYxLjQzLDE5NC43NCw5My4xMiwyMDgsMTI4LDIwOHM2Ni41Ny0xMy4yNiw5MS42Ni0zOC4zNGMxOC44My0xOC44MywyNy4zLTM3LjYxLDI3LjY1LTM4LjRBOCw4LDAsMCwwLDI0Ny4zMSwxMjQuNzZaTTEyOCwxOTJjLTMwLjc4LDAtNTcuNjctMTEuMTktNzkuOTMtMzMuMjVBMTMzLjQ3LDEzMy40NywwLDAsMSwyNSwxMjgsMTMzLjMzLDEzMy4zMywwLDAsMSw0OC4wNyw5Ny4yNUM3MC4zMyw3NS4xOSw5Ny4yMiw2NCwxMjgsNjRzNTcuNjcsMTEuMTksNzkuOTMsMzMuMjVBMTMzLjQ2LDEzMy40NiwwLDAsMSwyMzEuMDUsMTI4QzIyMy44NCwxNDEuNDYsMTkyLjQzLDE5MiwxMjgsMTkyWm0wLTExMmE0OCw0OCwwLDEsMCw0OCw0OEE0OC4wNSw0OC4wNSwwLDAsMCwxMjgsODBabTAsODBhMzIsMzIsMCwxLDEsMzItMzJBMzIsMzIsMCwwLDEsMTI4LDE2MFoiLz4nLAogICdmdW5jdGlvbic6ICc8cGF0aCBkPSJNMjAwLDQwVjIwMGExNiwxNiwwLDAsMS0xNiwxNkg1NlY1NkExNiwxNiwwLDAsMSw3Miw0MFoiIG9wYWNpdHk9IjAuMiIvPjxwYXRoIGQ9Ik0yMDgsNDBhOCw4LDAsMCwxLTgsOEgxNzAuNzFhMjQsMjQsMCwwLDAtMjMuNjIsMTkuNzFMMTM3LjU5LDEyMEgxODRhOCw4LDAsMCwxLDAsMTZIMTM0LjY4bC0xMCw1NS4xNkE0MCw0MCwwLDAsMSw4NS4yOSwyMjRINTZhOCw4LDAsMCwxLDAtMTZIODUuMjlhMjQsMjQsMCwwLDAsMjMuNjItMTkuNzFsOS41LTUyLjI5SDcyYTgsOCwwLDAsMSwwLTE2aDQ5LjMybDEwLTU1LjE2QTQwLDQwLDAsMCwxLDE3MC43MSwzMkgyMDBBOCw4LDAsMCwxLDIwOCw0MFoiLz4nLAogICdhZ2dyZWdhdGUnOiAnPHBhdGggZD0iTTE5Miw0OFYyMDhINjRsNjQtODBMNjQsNDhaIiBvcGFjaXR5PSIwLjIiLz48cGF0aCBkPSJNMTg0LDcyVjU2SDgwLjY1bDUzLjYsNjdhOCw4LDAsMCwxLDAsMTBsLTUzLjYsNjdIMTg0VjE4NGE4LDgsMCwwLDEsMTYsMHYyNGE4LDgsMCwwLDEtOCw4SDY0YTgsOCwwLDAsMS02LjI1LTEzbDYwLTc1LTYwLTc1QTgsOCwwLDAsMSw2NCw0MEgxOTJhOCw4LDAsMCwxLDgsOFY3MmE4LDgsMCwwLDEtMTYsMFoiLz4nLAogICd0aW8nOiAnPHBhdGggZD0iTTIyMS45LDYxLjM4LDE1MiwxMzZ2NTguNjVhOCw4LDAsMCwxLTMuNTYsNi42NmwtMzIsMjEuMzNBOCw4LDAsMCwxLDEwNCwyMTZWMTM2TDM0LjEsNjEuMzhBOCw4LDAsMCwxLDQwLDQ4SDIxNkE4LDgsMCwwLDEsMjIxLjksNjEuMzhaIiBvcGFjaXR5PSIwLjIiLz48cGF0aCBkPSJNMjMwLjYsNDkuNTNBMTUuODEsMTUuODEsMCwwLDAsMjE2LDQwSDQwQTE2LDE2LDAsMCwwLDI4LjE5LDY2Ljc2bC4wOC4wOUw5NiwxMzkuMTdWMjE2YTE2LDE2LDAsMCwwLDI0Ljg3LDEzLjMybDMyLTIxLjM0QTE2LDE2LDAsMCwwLDE2MCwxOTQuNjZWMTM5LjE3bDY3Ljc0LTcyLjMyLjA4LS4wOUExNS44LDE1LjgsMCwwLDAsMjMwLjYsNDkuNTNaTTQwLDU2aDBabTEwNi4xOCw3NC41OEE4LDgsMCwwLDAsMTQ0LDEzNnY1OC42NkwxMTIsMjE2VjEzNmE4LDgsMCwwLDAtMi4xNi01LjQ3TDQwLDU2SDIxNloiLz4nLAogICdjYXJldCc6ICc8cGF0aCBkPSJNMTgxLjY2LDEzMy42NmwtODAsODBhOCw4LDAsMCwxLTExLjMyLTExLjMyTDE2NC42OSwxMjgsOTAuMzQsNTMuNjZhOCw4LDAsMCwxLDExLjMyLTExLjMybDgwLDgwQTgsOCwwLDAsMSwxODEuNjYsMTMzLjY2WiIvPicsCiAgJ3NlYXJjaCc6ICc8cGF0aCBkPSJNMjI5LjY2LDIxOC4zNGwtNTAuMDctNTAuMDZhODguMTEsODguMTEsMCwxLDAtMTEuMzEsMTEuMzFsNTAuMDYsNTAuMDdhOCw4LDAsMCwwLDExLjMyLTExLjMyWk00MCwxMTJhNzIsNzIsMCwxLDEsNzIsNzJBNzIuMDgsNzIuMDgsMCwwLDEsNDAsMTEyWiIvPicsCiAgJ3JlcG8nOiAnPHBhdGggZD0iTTIzMiw2NGEzMiwzMiwwLDEsMC00MCwzMXYxN2E4LDgsMCwwLDEtOCw4SDk2YTIzLjg0LDIzLjg0LDAsMCwwLTgsMS4zOFY5NWEzMiwzMiwwLDEsMC0xNiwwdjY2YTMyLDMyLDAsMSwwLDE2LDBWMTQ0YTgsOCwwLDAsMSw4LThoODhhMjQsMjQsMCwwLDAsMjQtMjRWOTVBMzIuMDYsMzIuMDYsMCwwLDAsMjMyLDY0Wk02NCw2NEExNiwxNiwwLDEsMSw4MCw4MCwxNiwxNiwwLDAsMSw2NCw2NFpNOTYsMTkyYTE2LDE2LDAsMSwxLTE2LTE2QTE2LDE2LDAsMCwxLDk2LDE5MlpNMjAwLDgwYTE2LDE2LDAsMSwxLDE2LTE2QTE2LDE2LDAsMCwxLDIwMCw4MFoiLz4nLAogICdsaWNlbnNlJzogJzxwYXRoIGQ9Ik0yMzkuNDMsMTMzbC0zMi04MGgwYTgsOCwwLDAsMC05LjE2LTQuODRMMTM2LDYyVjQwYTgsOCwwLDAsMC0xNiwwVjY1LjU4TDU0LjI2LDgwLjE5QTgsOCwwLDAsMCw0OC41Nyw4NWgwdi4wNkwxNi41NywxNjVhNy45Miw3LjkyLDAsMCwwLS41NywzYzAsMjMuMzEsMjQuNTQsMzIsNDAsMzJzNDAtOC42OSw0MC0zMmE3LjkyLDcuOTIsMCwwLDAtLjU3LTNMNjYuOTIsOTMuNzcsMTIwLDgyVjIwOEgxMDRhOCw4LDAsMCwwLDAsMTZoNDhhOCw4LDAsMCwwLDAtMTZIMTM2Vjc4LjQyTDE4Nyw2Ny4xLDE2MC41NywxMzNhNy45Miw3LjkyLDAsMCwwLS41NywzYzAsMjMuMzEsMjQuNTQsMzIsNDAsMzJzNDAtOC42OSw0MC0zMkE3LjkyLDcuOTIsMCwwLDAsMjM5LjQzLDEzM1pNNTYsMTg0Yy03LjUzLDAtMjIuNzYtMy42MS0yMy45My0xNC42NEw1NiwxMDkuNTRsMjMuOTMsNTkuODJDNzguNzYsMTgwLjM5LDYzLjUzLDE4NCw1NiwxODRabTE0NC0zMmMtNy41MywwLTIyLjc2LTMuNjEtMjMuOTMtMTQuNjRMMjAwLDc3LjU0bDIzLjkzLDU5LjgyQzIyMi43NiwxNDguMzksMjA3LjUzLDE1MiwyMDAsMTUyWiIvPicsCiAgJ3N1cHBvcnQnOiAnPHBhdGggZD0iTTEyOCwyNEExMDQsMTA0LDAsMSwwLDIzMiwxMjgsMTA0LjExLDEwNC4xMSwwLDAsMCwxMjgsMjRabTM5LjEsMTMxLjc5YTQ3Ljg0LDQ3Ljg0LDAsMCwwLDAtNTUuNThsMjguNS0yOC40OWE4Ny44Myw4Ny44MywwLDAsMSwwLDExMi41NlpNOTYsMTI4YTMyLDMyLDAsMSwxLDMyLDMyQTMyLDMyLDAsMCwxLDk2LDEyOFptODguMjgtNjcuNkwxNTUuNzksODguOWE0Ny44NCw0Ny44NCwwLDAsMC01NS41OCwwTDcxLjcyLDYwLjRhODcuODMsODcuODMsMCwwLDEsMTEyLjU2LDBaTTYwLjQsNzEuNzJsMjguNSwyOC40OWE0Ny44NCw0Ny44NCwwLDAsMCwwLDU1LjU4TDYwLjQsMTg0LjI4YTg3LjgzLDg3LjgzLDAsMCwxLDAtMTEyLjU2Wk03MS43MiwxOTUuNmwyOC40OS0yOC41YTQ3Ljg0LDQ3Ljg0LDAsMCwwLDU1LjU4LDBsMjguNDksMjguNWE4Ny44Myw4Ny44MywwLDAsMS0xMTIuNTYsMFoiLz4nLAogICdwb2xpY3knOiAnPHBhdGggZD0iTTIwOCw0MEg0OEExNiwxNiwwLDAsMCwzMiw1NnY1NmMwLDUyLjcyLDI1LjUyLDg0LjY3LDQ2LjkzLDEwMi4xOSwyMy4wNiwxOC44Niw0NiwyNS4yNiw0NywyNS41M2E4LDgsMCwwLDAsNC4yLDBjMS0uMjcsMjMuOTEtNi42Nyw0Ny0yNS41M0MxOTguNDgsMTk2LjY3LDIyNCwxNjQuNzIsMjI0LDExMlY1NkExNiwxNiwwLDAsMCwyMDgsNDBabTAsNzJjMCwzNy4wNy0xMy42Niw2Ny4xNi00MC42LDg5LjQyQTEyOS4zLDEyOS4zLDAsMCwxLDEyOCwyMjMuNjJhMTI4LjI1LDEyOC4yNSwwLDAsMS0zOC45Mi0yMS44MUM2MS44MiwxNzkuNTEsNDgsMTQ5LjMsNDgsMTEybDAtNTYsMTYwLDBaTTgyLjM0LDE0MS42NmE4LDgsMCwwLDEsMTEuMzItMTEuMzJMMTEyLDE0OC42OWw1MC4zNC01MC4zNWE4LDgsMCwwLDEsMTEuMzIsMTEuMzJsLTU2LDU2YTgsOCwwLDAsMS0xMS4zMiwwWiIvPicsCiAgJ2tleXdvcmQnOiAnPHBhdGggZD0iTTI0My4zMSwxMzYsMTQ0LDM2LjY5QTE1Ljg2LDE1Ljg2LDAsMCwwLDEzMi42OSwzMkg0MGE4LDgsMCwwLDAtOCw4djkyLjY5QTE1Ljg2LDE1Ljg2LDAsMCwwLDM2LjY5LDE0NEwxMzYsMjQzLjMxYTE2LDE2LDAsMCwwLDIyLjYzLDBsODQuNjgtODQuNjhhMTYsMTYsMCwwLDAsMC0yMi42M1ptLTk2LDk2TDQ4LDEzMi42OVY0OGg4NC42OUwyMzIsMTQ3LjMxWk05Niw4NEExMiwxMiwwLDEsMSw4NCw3MiwxMiwxMiwwLDAsMSw5Niw4NFoiLz4nLAogICdzcGFya2xlJzogJzxwYXRoIGQ9Ik0yMDgsMTQ0YTE1Ljc4LDE1Ljc4LDAsMCwxLTEwLjQyLDE0Ljk0bC01MS42NSwxOS0xOSw1MS42MWExNS45MiwxNS45MiwwLDAsMS0yOS44OCwwTDc4LDE3OGwtNTEuNjItMTlhMTUuOTIsMTUuOTIsMCwwLDEsMC0yOS44OEw3OCwxMTAuMDVsMTktNTEuNjFhMTUuOTIsMTUuOTIsMCwwLDEsMjkuODgsMGwxOSw1MS42NSw1MS42MSwxOUExNS43OCwxNS43OCwwLDAsMSwyMDgsMTQ0WiIgb3BhY2l0eT0iMC4yIi8+PHBhdGggZD0iTTE5Ny41OCwxMjkuMDYsMTQ2LDExMGwtMTktNTEuNjJhMTUuOTIsMTUuOTIsMCwwLDAtMjkuODgsMEw3OCwxMTAsMjYuNDIsMTI5YTE1LjkyLDE1LjkyLDAsMCwwLDAsMjkuODhMNzgsMTc4bDE5LDUxLjYyYTE1LjkyLDE1LjkyLDAsMCwwLDI5Ljg4LDBMMTQ2LDE3OGw1MS42Mi0xOWExNS45MiwxNS45MiwwLDAsMCwwLTI5Ljg4Wk0xMzcsMTY0YTE1LjksMTUuOSwwLDAsMC05LjQ0LDkuNDVMMTEyLDIxNS42Niw5Ni40OSwxNzMuNEExNS45LDE1LjksMCwwLDAsODcsMTY0TDQ0Ljc5LDE0OC41LDg3LDEzM2ExNS45LDE1LjksMCwwLDAsOS40NS05LjQ0TDExMiw4MS4zNGwxNS41MSw0Mi4yMkExNS45LDE1LjksMCwwLDAsMTM3LDEzM2w0Mi4yMiwxNS41WiIvPicsCiAgJ2Fycm93JzogJzxwYXRoIGQ9Ik0yMjEuNjYsMTMzLjY2bC03Miw3MmE4LDgsMCwwLDEtMTEuMzItMTEuMzJMMTk2LjY5LDEzNkg0MGE4LDgsMCwwLDEsMC0xNkgxOTYuNjlMMTM4LjM0LDYxLjY2YTgsOCwwLDAsMSwxMS4zMi0xMS4zMmw3Miw3MkE4LDgsMCwwLDEsMjIxLjY2LDEzMy42NloiLz4nCn07CmZ1bmN0aW9uIGljb24obmFtZSwgY2xzKXsgcmV0dXJuICc8c3ZnIGNsYXNzPSJpYyAnKyhjbHN8fCcnKSsnIiB2aWV3Qm94PSIwIDAgMjU2IDI1NiIgZmlsbD0iY3VycmVudENvbG9yIiBhcmlhLWhpZGRlbj0idHJ1ZSI+JysoSUNPTlNbbmFtZV18fCcnKSsnPC9zdmc+JzsgfQoKLy8g4pSA4pSAIFNpbXVsYXRlZCBHRVQge3ByZWZpeH0vZGVzY3JpYmUuanNvbiDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKbGV0IGRlc2NyaWJlID0gbnVsbDsKY29uc3QgU1VQUE9SVEVEX1NDSEVNQSA9IDE7CgpmdW5jdGlvbiBmZXRjaENvbHVtbnMoY2F0TmFtZSwgc2NoZW1hLCB0YWJsZSl7CiAgcmV0dXJuIGZldGNoKCdkZXNjcmliZS8nK2VuY29kZVVSSUNvbXBvbmVudChjYXROYW1lKSsnLycrZW5jb2RlVVJJQ29tcG9uZW50KHNjaGVtYSkrJy8nK2VuY29kZVVSSUNvbXBvbmVudCh0YWJsZSkrJy5qc29uJywKICAgIHtoZWFkZXJzOnsnQWNjZXB0JzonYXBwbGljYXRpb24vanNvbid9fSkKICAgIC50aGVuKHI9PnIub2sgPyByLmpzb24oKSA6IHtjb2x1bW5zOltdfSkuY2F0Y2goKCk9Pih7Y29sdW1uczpbXX0pKTsKfQoKY29uc3Qgb3JpZ2luID0gbG9jYXRpb24ub3JpZ2luOwpjb25zdCBiYXNlUGF0aCA9IGxvY2F0aW9uLnBhdGhuYW1lLmVuZHNXaXRoKCcvJykgPyBsb2NhdGlvbi5wYXRobmFtZSA6IGxvY2F0aW9uLnBhdGhuYW1lICsgJy8nOwpjb25zdCBwcmVmaXggPSBiYXNlUGF0aC5yZXBsYWNlKC9cLyQvLCAnJyk7CmNvbnN0IHNlcnZpY2VVcmwgPSBvcmlnaW4gKyBiYXNlUGF0aDsKY29uc3QgZXNjID0gcyA9PiBTdHJpbmcocykucmVwbGFjZSgvWyY8PiJdL2csIGM9Pih7JyYnOicmYW1wOycsJzwnOicmbHQ7JywnPic6JyZndDsnLCciJzonJnF1b3Q7J31bY10pKTsKY29uc3Qgc3FsSGkgPSBzID0+IGVzYyhzKS5yZXBsYWNlKC9cYihTRUxFQ1R8RlJPTXxXSEVSRXxHUk9VUCBCWXxPUkRFUiBCWXxJTlRFUlZBTHxBTkR8T1J8QVN8Sk9JTnxPTnxZRUFSKVxiL2csIG09Pic8c3BhbiBjbGFzcz0ia3ciPicrbSsnPC9zcGFuPicpOwoKZnVuY3Rpb24gbWRUb0h0bWwoc3JjKXsKICBpZighc3JjKSByZXR1cm4gJyc7CiAgdmFyIGxpbmVzID0gU3RyaW5nKHNyYykucmVwbGFjZSgvXHJcbj8vZywnXG4nKS5zcGxpdCgnXG4nKSwgb3V0PVtdLCBpPTA7CiAgZnVuY3Rpb24gaW5sKHQpewogICAgdCA9IGVzYyh0KTsKICAgIHQgPSB0LnJlcGxhY2UoL2AoW15gXSspYC9nLCc8Y29kZT4kMTwvY29kZT4nKTsKICAgIHQgPSB0LnJlcGxhY2UoL1wqXCooW14qXSspXCpcKi9nLCc8c3Ryb25nPiQxPC9zdHJvbmc+Jyk7CiAgICB0ID0gdC5yZXBsYWNlKC8oXnxbXipdKVwqKFteKl0rKVwqL2csJyQxPGVtPiQyPC9lbT4nKTsKICAgIHQgPSB0LnJlcGxhY2UoL1xbKFteXF1dKylcXVwoKGh0dHBzPzpcL1wvW14pXHNdKylcKS9nLCc8YSBocmVmPSIkMiIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPiQxPC9hPicpOwogICAgcmV0dXJuIHQ7CiAgfQogIC8vIEEgR0ZNIHRhYmxlID0gYSByb3cgY29udGFpbmluZyAnfCcgaW1tZWRpYXRlbHkgZm9sbG93ZWQgYnkgYSBkZWxpbWl0ZXIgcm93CiAgLy8gKHBpcGVzIG9mIGRhc2hlcyB3aXRoIG9wdGlvbmFsIGFsaWdubWVudCBjb2xvbnMpLgogIHZhciBpc0RlbGltID0gZnVuY3Rpb24ocyl7IHJldHVybiBzIT1udWxsICYmIC9eXHMqXHw/XHMqOj8tKzo/XHMqKFx8XHMqOj8tKzo/XHMqKSpcfD9ccyokLy50ZXN0KHMpICYmIHMuaW5kZXhPZignLScpPj0wOyB9OwogIHZhciBpc1RhYmxlU3RhcnQgPSBmdW5jdGlvbihpZHgpeyByZXR1cm4gaWR4PGxpbmVzLmxlbmd0aCAmJiBsaW5lc1tpZHhdLmluZGV4T2YoJ3wnKT49MCAmJiBpc0RlbGltKGxpbmVzW2lkeCsxXSk7IH07CiAgdmFyIHNwbGl0Um93ID0gZnVuY3Rpb24ocil7IHJldHVybiByLnRyaW0oKS5yZXBsYWNlKC9eXHwvLCcnKS5yZXBsYWNlKC9cfCQvLCcnKS5zcGxpdCgnfCcpLm1hcChmdW5jdGlvbihjKXtyZXR1cm4gYy50cmltKCk7fSk7IH07CiAgd2hpbGUoaTxsaW5lcy5sZW5ndGgpewogICAgdmFyIGxuPWxpbmVzW2ldOwogICAgaWYoL15ccyokLy50ZXN0KGxuKSl7IGkrKzsgY29udGludWU7IH0KICAgIHZhciBoPWxuLm1hdGNoKC9eKCN7MSw2fSlccysoLiopJC8pOwogICAgaWYoaCl7IHZhciBsdj1NYXRoLm1pbihoWzFdLmxlbmd0aCw2KTsgb3V0LnB1c2goJzxoJytsdisnIGNsYXNzPSJtZC1oIj4nK2lubChoWzJdKSsnPC9oJytsdisnPicpOyBpKys7IGNvbnRpbnVlOyB9CiAgICBpZihpc1RhYmxlU3RhcnQoaSkpewogICAgICB2YXIgaGVhZHM9c3BsaXRSb3cobG4pOwogICAgICB2YXIgYWxpZ25zPXNwbGl0Um93KGxpbmVzW2krMV0pLm1hcChmdW5jdGlvbihzKXsgdmFyIGw9cy5jaGFyQXQoMCk9PT0nOicsIHI9cy5jaGFyQXQocy5sZW5ndGgtMSk9PT0nOic7IHJldHVybiAobCYmcik/J2NlbnRlcic6cj8ncmlnaHQnOmw/J2xlZnQnOicnOyB9KTsKICAgICAgaSs9MjsKICAgICAgdmFyIHJvd3M9W107CiAgICAgIHdoaWxlKGk8bGluZXMubGVuZ3RoICYmIGxpbmVzW2ldLmluZGV4T2YoJ3wnKT49MCAmJiAhL15ccyokLy50ZXN0KGxpbmVzW2ldKSl7IHJvd3MucHVzaChzcGxpdFJvdyhsaW5lc1tpXSkpOyBpKys7IH0KICAgICAgdmFyIGNlbGw9ZnVuY3Rpb24odGFnLHR4dCxjaSl7IHZhciBhPWFsaWduc1tjaV0/JyBzdHlsZT0idGV4dC1hbGlnbjonK2FsaWduc1tjaV0rJyInOicnOyByZXR1cm4gJzwnK3RhZythKyc+JytpbmwodHh0fHwnJykrJzwvJyt0YWcrJz4nOyB9OwogICAgICB2YXIgdGhlYWQ9Jzx0cj4nK2hlYWRzLm1hcChmdW5jdGlvbihjLGNpKXtyZXR1cm4gY2VsbCgndGgnLGMsY2kpO30pLmpvaW4oJycpKyc8L3RyPic7CiAgICAgIHZhciB0Ym9keT1yb3dzLm1hcChmdW5jdGlvbihyb3cpe3JldHVybiAnPHRyPicraGVhZHMubWFwKGZ1bmN0aW9uKF8sY2kpe3JldHVybiBjZWxsKCd0ZCcscm93W2NpXSxjaSk7fSkuam9pbignJykrJzwvdHI+Jzt9KS5qb2luKCcnKTsKICAgICAgb3V0LnB1c2goJzxkaXYgY2xhc3M9Im1kLXRhYmxld3JhcCI+PHRhYmxlIGNsYXNzPSJtZC10YWJsZSI+PHRoZWFkPicrdGhlYWQrJzwvdGhlYWQ+PHRib2R5PicrdGJvZHkrJzwvdGJvZHk+PC90YWJsZT48L2Rpdj4nKTsKICAgICAgY29udGludWU7CiAgICB9CiAgICBpZigvXlxzKlstKitdXHMrLy50ZXN0KGxuKSl7CiAgICAgIHZhciBpdGVtcz1bXTsKICAgICAgd2hpbGUoaTxsaW5lcy5sZW5ndGggJiYgL15ccypbLSorXVxzKy8udGVzdChsaW5lc1tpXSkpeyBpdGVtcy5wdXNoKCc8bGk+JytpbmwobGluZXNbaV0ucmVwbGFjZSgvXlxzKlstKitdXHMrLywnJykpKyc8L2xpPicpOyBpKys7IH0KICAgICAgb3V0LnB1c2goJzx1bCBjbGFzcz0ibWQtdWwiPicraXRlbXMuam9pbignJykrJzwvdWw+Jyk7IGNvbnRpbnVlOwogICAgfQogICAgdmFyIHBhcmE9W2xuXTsgaSsrOwogICAgd2hpbGUoaTxsaW5lcy5sZW5ndGggJiYgIS9eXHMqJC8udGVzdChsaW5lc1tpXSkgJiYgIS9eKCN7MSw2fVxzfFxzKlstKitdXHMpLy50ZXN0KGxpbmVzW2ldKSAmJiAhaXNUYWJsZVN0YXJ0KGkpKXsgcGFyYS5wdXNoKGxpbmVzW2ldKTsgaSsrOyB9CiAgICBvdXQucHVzaCgnPHAgY2xhc3M9Im1kLXAiPicraW5sKHBhcmEuam9pbignICcpKSsnPC9wPicpOwogIH0KICByZXR1cm4gb3V0LmpvaW4oJycpOwp9CnZhciBfRERCX1RZUEVTID0ge2ludDg6J1RJTllJTlQnLGludDE2OidTTUFMTElOVCcsaW50MzI6J0lOVEVHRVInLGludDY0OidCSUdJTlQnLAogIHVpbnQ4OidVVElOWUlOVCcsdWludDE2OidVU01BTExJTlQnLHVpbnQzMjonVUlOVEVHRVInLHVpbnQ2NDonVUJJR0lOVCcsCiAgaGFsZmZsb2F0OidGTE9BVCcsZmxvYXQ6J0ZMT0FUJyxmbG9hdDMyOidGTE9BVCcsZG91YmxlOidET1VCTEUnLGZsb2F0NjQ6J0RPVUJMRScsCiAgYm9vbDonQk9PTEVBTicsYm9vbGVhbjonQk9PTEVBTicsc3RyaW5nOidWQVJDSEFSJyxsYXJnZV9zdHJpbmc6J1ZBUkNIQVInLHV0Zjg6J1ZBUkNIQVInLGxhcmdlX3V0Zjg6J1ZBUkNIQVInLAogIGJpbmFyeTonQkxPQicsbGFyZ2VfYmluYXJ5OidCTE9CJyxkYXRlMzI6J0RBVEUnLCdkYXRlMzJbZGF5XSc6J0RBVEUnLGRhdGU2NDonREFURScsbnVsbDonTlVMTCcsCiAgbW9udGhfZGF5X25hbm9faW50ZXJ2YWw6J0lOVEVSVkFMJ307CmZ1bmN0aW9uIGR1Y2tkYlR5cGUodCl7CiAgaWYoIXQpIHJldHVybiB0OyB0PVN0cmluZyh0KTsgdmFyIGx0PXQudG9Mb3dlckNhc2UoKTsKICBpZihfRERCX1RZUEVTW2x0XSkgcmV0dXJuIF9EREJfVFlQRVNbbHRdOwogIHZhciBkPXQubWF0Y2goL15kaWN0aW9uYXJ5PHZhbHVlcz0oW14sPl0rKS9pKTsgaWYoZCkgcmV0dXJuIGR1Y2tkYlR5cGUoZFsxXS50cmltKCkpOwogIHZhciBsPXQubWF0Y2goL14oPzpsYXJnZV8pP2xpc3Q8KD86aXRlbTpccyopPyguKyk+JC9pKTsgaWYobCkgcmV0dXJuIGR1Y2tkYlR5cGUobFsxXS50cmltKCkpKydbXSc7CiAgaWYoL15maXhlZF9zaXplX2JpbmFyeS9pLnRlc3QodCkpIHJldHVybiAnQkxPQic7CiAgdmFyIHRzPXQubWF0Y2goL150aW1lc3RhbXBcWyhbXixcXV0rKSg/Oixccyp0ej0oW15cXV0rKSk/XF0kL2kpOyBpZih0cykgcmV0dXJuIHRzWzJdPydUSU1FU1RBTVBUWic6J1RJTUVTVEFNUCc7CiAgaWYoL150aW1lKDMyfDY0KVxbL2kudGVzdCh0KSkgcmV0dXJuICdUSU1FJzsKICBpZigvXmR1cmF0aW9uXFsvaS50ZXN0KHQpKSByZXR1cm4gJ0lOVEVSVkFMJzsKICB2YXIgZGVjPXQubWF0Y2goL15kZWNpbWFsKD86MTI4fDI1NilcKChcZCspLFxzKihcZCspXCkkL2kpOyBpZihkZWMpIHJldHVybiAnREVDSU1BTCgnK2RlY1sxXSsnLCcrZGVjWzJdKycpJzsKICBpZigvXnN0cnVjdDwvaS50ZXN0KHQpKSByZXR1cm4gJ1NUUlVDVCc7CiAgaWYoL15tYXA8L2kudGVzdCh0KSkgcmV0dXJuICdNQVAnOwogIHJldHVybiB0Owp9CgoKLy8g4pSA4pSAIGlkZW50aXR5IHBpbGwg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACmZ1bmN0aW9uIHJlbmRlcklkZW50aXR5KCl7CiAgaWYgKCFkZXNjcmliZS5vYXV0aCkgcmV0dXJuOwogIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3ZnaS11c2VyLWluZm8nKTsKICBjb25zdCBtID0gZG9jdW1lbnQuY29va2llLm1hdGNoKCcoXnw7KVxccypfdmdpX2lkZW50aXR5PShbXjtdKyknKTsKICBpZiAoIW0pIHsgZWwuY2xhc3NMaXN0LmFkZCgnc2lnbmluJyk7IGVsLmlubmVySFRNTCA9ICc8c3BhbiBjbGFzcz0iYXZhdGFyIj4/PC9zcGFuPjxhIGNsYXNzPSJhY3QiIGhyZWY9IicrcHJlZml4KycvX29hdXRoL2xvZ2luIj5TaWduIGluPC9hPic7IHJldHVybjsgfQogIGxldCBwPXt9OyB0cnkgeyBwID0gSlNPTi5wYXJzZShhdG9iKG1bMl0ucmVwbGFjZSgvLS9nLCcrJykucmVwbGFjZSgvXy9nLCcvJykpKTsgfSBjYXRjaChlKXt9CiAgY29uc3QgZGlzcGxheSA9IHAuZW1haWwgfHwgcC5wcmVmZXJyZWRfdXNlcm5hbWUgfHwgcC5uYW1lIHx8IHAuc3ViIHx8ICcnOwogIGNvbnN0IHNyYyA9IChwLm5hbWUgfHwgcC5lbWFpbCB8fCAnPycpLnRyaW0oKTsKICBjb25zdCBpbml0aWFscyA9IChzcmMuc3BsaXQoL1tcc0AuXy1dKy8pLmZpbHRlcihCb29sZWFuKS5zbGljZSgwLDIpLm1hcCh3PT53WzBdKS5qb2luKCcnKSB8fCAnPycpLnRvVXBwZXJDYXNlKCk7CiAgZWwuaW5uZXJIVE1MID0gKHAucGljdHVyZSA/ICc8aW1nIGNsYXNzPSJhdmF0YXIiIHNyYz0iJytlc2MocC5waWN0dXJlKSsnIiBhbHQ9IiI+JyA6ICc8c3BhbiBjbGFzcz0iYXZhdGFyIj4nK2VzYyhpbml0aWFscykrJzwvc3Bhbj4nKSArCiAgICAnPHNwYW4gY2xhc3M9ImVtYWlsIiB0aXRsZT0iJytlc2MoZGlzcGxheSkrJyI+Jytlc2MoZGlzcGxheSkrJzwvc3Bhbj48YSBjbGFzcz0iYWN0IiBocmVmPSInK3ByZWZpeCsnL19vYXV0aC9sb2dvdXQiPlNpZ24gb3V0PC9hPic7Cn0KCi8vIOKUgOKUgCBoZWFkZXIg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACmZ1bmN0aW9uIHJlbmRlckhlYWRlcigpewogIGRvY3VtZW50LnRpdGxlID0gZGVzY3JpYmUud29ya2VyLm5hbWUgKyAnIFx1MDBiNyBWR0knOwogIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd3b3JrZXItbmFtZScpLnRleHRDb250ZW50ID0gZGVzY3JpYmUud29ya2VyLm5hbWU7CiAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3dvcmtlci1kb2MnKS50ZXh0Q29udGVudCA9IGRlc2NyaWJlLndvcmtlci5kb2MgfHwgJyc7Cn0KCi8vIOKUgOKUgCBzdGF0ZSDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKbGV0IGFjdGl2ZSA9IDA7CmNvbnN0IHNlbGVjdGVkID0ge307CmNvbnN0IGNhdCA9ICgpID0+IGRlc2NyaWJlLmNhdGFsb2dzW2FjdGl2ZV07CmNvbnN0IGxhdGVzdFNwZWMgPSBjID0+IChjLmRhdGFfdmVyc2lvbnMgJiYgYy5kYXRhX3ZlcnNpb25zWzBdKSA/IGMuZGF0YV92ZXJzaW9uc1swXS5zcGVjIDogbnVsbDsKY29uc3QgY3VycmVudFNwZWMgPSBjID0+IHNlbGVjdGVkW2MubmFtZV07CmZ1bmN0aW9uIGN1cG9sYVVybChoYXNoKXsKICBjb25zdCBjID0gY2F0KCk7IGxldCB1ID0gZGVzY3JpYmUuY3Vwb2xhX2Jhc2UrIi8/c2VydmljZT0iK2VuY29kZVVSSUNvbXBvbmVudChzZXJ2aWNlVXJsKTsKICBjb25zdCB2ID0gY3VycmVudFNwZWMoYyk7IGlmICh2KSB1ICs9ICImZGF0YV92ZXJzaW9uX3NwZWM9IitlbmNvZGVVUklDb21wb25lbnQodik7CiAgcmV0dXJuIHUgKyAoaGFzaHx8IiIpOwp9CgovLyDilIDilIAgY2F0YWxvZyBwaWNrZXIg4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSA4pSACmZ1bmN0aW9uIHJlbmRlckNhdEJhcigpewogIGNvbnN0IGJhciA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjYXRiYXInKTsKICBpZiAoZGVzY3JpYmUuY2F0YWxvZ3MubGVuZ3RoIDwgMil7IGJhci5zdHlsZS5kaXNwbGF5PSdub25lJzsgcmV0dXJuOyB9CiAgYmFyLmlubmVySFRNTCA9ICc8c3BhbiBjbGFzcz0ibGFiZWwiPkNhdGFsb2dzPC9zcGFuPjxkaXYgY2xhc3M9ImNhdC10YWJzIj4nICsKICAgIGRlc2NyaWJlLmNhdGFsb2dzLm1hcCgoYyxpKT0+ewogICAgICBjb25zdCBudiA9IChjLmRhdGFfdmVyc2lvbnMgJiYgYy5kYXRhX3ZlcnNpb25zLmxlbmd0aCkgPyBjLmRhdGFfdmVyc2lvbnMubGVuZ3RoKycgdmVyc2lvbnMnIDogJ3VudmVyc2lvbmVkJzsKICAgICAgcmV0dXJuICc8YnV0dG9uIGNsYXNzPSJjYXQtdGFiJysoaT09PWFjdGl2ZT8nIGFjdGl2ZSc6JycpKyciIGRhdGEtaT0iJytpKyciPicrZXNjKGMubmFtZSkrJyA8c3BhbiBjbGFzcz0iY3YiPicrbnYrJzwvc3Bhbj48L2J1dHRvbj4nOwogICAgfSkuam9pbignJykgKyAnPC9kaXY+JzsKICBiYXIucXVlcnlTZWxlY3RvckFsbCgnLmNhdC10YWInKS5mb3JFYWNoKGI9PmIuYWRkRXZlbnRMaXN0ZW5lcignY2xpY2snLCgpPT57IGFjdGl2ZT0rYi5kYXRhc2V0Lmk7IHJlbmRlckFsbCgpOyB9KSk7Cn0KCi8vIOKUgOKUgCBjYXRhbG9nIG1ldGFkYXRhIGNhcmQgKHZnaS4qIHRhZ3MpIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgApmdW5jdGlvbiByZW5kZXJDYXRDYXJkKCl7CiAgY29uc3QgYyA9IGNhdCgpLCB0ID0gYy50YWdzIHx8IHt9OwogIGNvbnN0IGJhZGdlID0gKGhyZWYsIGljLCBsYWJlbCwgdmFsKSA9PiB7CiAgICBjb25zdCBpbm5lciA9IGljb24oaWMpKyc8c3Bhbj4nK2xhYmVsKyh2YWw/JyA8Yj4nK2VzYyh2YWwpKyc8L2I+JzonJykrJzwvc3Bhbj4nOwogICAgcmV0dXJuIGhyZWYgPyAnPGEgY2xhc3M9ImJhZGdlJysoaWM9PT0nbGljZW5zZSc/JyBsaWNlbnNlJzonJykrJyIgaHJlZj0iJytlc2MoaHJlZikrJyIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPicraW5uZXIrJzwvYT4nCiAgICAgICAgICAgICAgICA6ICc8c3BhbiBjbGFzcz0iYmFkZ2UnKyhpYz09PSdsaWNlbnNlJz8nIGxpY2Vuc2UnOicnKSsnIj4nK2lubmVyKyc8L3NwYW4+JzsKICB9OwogIGxldCBiYWRnZXMgPSAnJzsKICBpZiAodC5zb3VyY2VfdXJsKSBiYWRnZXMgKz0gYmFkZ2UodC5zb3VyY2VfdXJsLCAncmVwbycsICdSZXBvc2l0b3J5Jyk7CiAgaWYgKHQubGljZW5zZSkgYmFkZ2VzICs9IGJhZGdlKG51bGwsICdsaWNlbnNlJywgJ0xpY2Vuc2UnLCB0LmxpY2Vuc2UpOwogIGlmICh0LnN1cHBvcnRfY29udGFjdCkgYmFkZ2VzICs9IGJhZGdlKHQuc3VwcG9ydF9jb250YWN0LCAnc3VwcG9ydCcsICdTdXBwb3J0Jyk7CiAgaWYgKHQuc3VwcG9ydF9wb2xpY3lfdXJsKSBiYWRnZXMgKz0gYmFkZ2UodC5zdXBwb3J0X3BvbGljeV91cmwsICdwb2xpY3knLCAnU3VwcG9ydCBwb2xpY3knKTsKICBpZiAoYy5pbXBsZW1lbnRhdGlvbl92ZXJzaW9uKSBiYWRnZXMgKz0gJzxzcGFuIGNsYXNzPSJiYWRnZSIgdGl0bGU9IlZlcnNpb24gb2YgdGhlIGNhdGFsb2cgaW1wbGVtZW50YXRpb24g4oCUIHRoZSB3b3JrZXIgY29kZSBzZXJ2aW5nIHRoaXMgY2F0YWxvZywgaW5kZXBlbmRlbnQgb2YgdGhlIGRhdGEgdmVyc2lvbiBiZWxvdy4iPicraWNvbignaW1wbCcpKyc8c3Bhbj5JbXBsZW1lbnRhdGlvbiA8Yj52Jytlc2MoYy5pbXBsZW1lbnRhdGlvbl92ZXJzaW9uKSsnPC9iPjwvc3Bhbj48L3NwYW4+JzsKICBjb25zdCBrdyA9ICh0LmtleXdvcmRzICYmIHQua2V5d29yZHMubGVuZ3RoKQogICAgPyAnPGRpdiBjbGFzcz0iY2Mta3ciPicraWNvbigna2V5d29yZCcpK3Qua2V5d29yZHMubWFwKGs9Pic8c3BhbiBjbGFzcz0iayI+Jytlc2MoaykrJzwvc3Bhbj4nKS5qb2luKCcnKSsnPC9kaXY+JyA6ICcnOwogIGNvbnN0IGJ5ID0gKHQuYXV0aG9yIHx8IHQuY29weXJpZ2h0KQogICAgPyAnPGRpdiBjbGFzcz0iY2MtYnkiPicrW3QuYXV0aG9yPydieSAnK2VzYyh0LmF1dGhvcik6JycsIHQuY29weXJpZ2h0P2VzYyh0LmNvcHlyaWdodCk6JyddLmZpbHRlcihCb29sZWFuKS5qb2luKCcgwrcgJykrJzwvZGl2PicgOiAnJzsKICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnY2F0LWNhcmQnKS5pbm5lckhUTUwgPQogICAgJzxkaXYgY2xhc3M9ImNjLXRpdGxlIj4nK2ljb24oJ2NhdGFsb2cnKStlc2ModC50aXRsZSB8fCBjLm5hbWUpKyc8L2Rpdj4nICsKICAgICh0LmRvY19tZCA/ICc8ZGl2IGNsYXNzPSJjYy1kb2MiPicrbWRUb0h0bWwodC5kb2NfbWQpKyc8L2Rpdj4nIDogJycpICsKICAgIChiYWRnZXMgPyAnPGRpdiBjbGFzcz0iYmFkZ2VzIj4nK2JhZGdlcysnPC9kaXY+JyA6ICcnKSArIGt3ICsgYnk7Cn0KCi8vIOKUgOKUgCBjb25uZWN0OiB0YWJiZWQgRHVja0RCIC8gUHl0aG9uIC8gTm9kZS5qcyArIGRhdGEtdmVyc2lvbiArIGF0dGFjaCBvcHRpb25zIOKUgOKUgOKUgOKUgOKUgApsZXQgY29ubmVjdFRhYiA9IDA7CmNvbnN0IENPTk5FQ1RfVEFCUyA9IFtbJ3NxbCcsJ0R1Y2tEQicsJ3RhYl9kdWNrZGInXSxbJ3B5dGhvbicsJ1B5dGhvbicsJ3RhYl9weXRob24nXSxbJ2pzJywnTm9kZS5qcycsJ3RhYl9ub2RlJ11dOwpjb25zdCBUQUJJQ09OUyA9IHsKICB0YWJfZHVja2RiOiAnPGltZyBjbGFzcz0idGFiLWxvZ28iIHNyYz0iZGF0YTppbWFnZS9zdmcreG1sO2Jhc2U2NCxQRDk0Yld3Z2RtVnljMmx2YmowaU1TNHdJaUJsYm1OdlpHbHVaejBpVlZSR0xUZ2lQejRLUEhOMlp5QnBaRDBpUldKbGJtVmZNU0lnZUcxc2JuTTlJbWgwZEhBNkx5OTNkM2N1ZHpNdWIzSm5Mekl3TURBdmMzWm5JaUIyWlhKemFXOXVQU0l4TGpFaUlIWnBaWGRDYjNnOUlqQWdNQ0ExTURBZ05UQXdJajRLSUNBOElTMHRJRWRsYm1WeVlYUnZjam9nUVdSdlltVWdTV3hzZFhOMGNtRjBiM0lnTWprdU1DNHdMQ0JUVmtjZ1JYaHdiM0owSUZCc2RXY3RTVzRnTGlCVFZrY2dWbVZ5YzJsdmJqb2dNaTR4TGpBZ1FuVnBiR1FnTVRnMktTQWdMUzArQ2lBZ1BHUmxabk0rQ2lBZ0lDQThjM1I1YkdVK0NpQWdJQ0FnSUM1emREQWdld29nSUNBZ0lDQWdJR1pwYkd3NklDTm1abVl4TURBN0NpQWdJQ0FnSUgwS0NpQWdJQ0FnSUM1emRERWdld29nSUNBZ0lDQWdJR1pwYkd3NklDTXhZVEZoTVdFN0NpQWdJQ0FnSUgwS0lDQWdJRHd2YzNSNWJHVStDaUFnUEM5a1pXWnpQZ29nSUR4d1lYUm9JR05zWVhOelBTSnpkREVpSUdROUlrMHlORGt1T1RrNU5qY3pOeXcxTURCRE1URXhMamt6TWpBeU5qY3NOVEF3TERBc016ZzRMakEyT0RReU5Td3dMREkxTUM0d01EQXdNalV4TERBc01URXhMamt6TVRneU5Ua3NNVEV4TGpNMk16Y3pOeklzTUN3eU5Ea3VPVGs1Tmpjek55d3dMRE00T0M0Mk16WTNNVFExTERBc05UQXdMREV4TVM0NU16RTRNalU1TERVd01Dd3lOVEF1TURBd01ESTFNV013TERFek9DNHdOamd6T1RrNUxURXhNUzQ1TXpFM05UQTJMREkwT1M0NU9UazVOelE1TFRJMU1DNHdNREF6TWpZekxESTBPUzQ1T1RrNU56UTVXaUl2UGdvZ0lEeG5QZ29nSUNBZ1BIQmhkR2dnWTJ4aGMzTTlJbk4wTUNJZ1pEMGlUVEU1TUM0d05UUTFNRFExTERFME5pNDFPVEEzTnpJMFl5MDFOaTQ0TVRnME56STNMREF0TVRBekxqUXdPRGs0TWprc05EWXVOVGt3T0RReU55MHhNRE11TkRBNE9UZ3lPU3d4TURNdU5EQTVNakkzTml3d0xEVTNMak00TmpnMU1ERXNORFl1TlRrd05URXdNaXd4TURNdU5EQTVNalF3TVN3eE1ETXVOREE0T1RneU9Td3hNRE11TkRBNU1qUXdNU3cxTmk0NE1UZ3pPVGMwTERBc01UQXpMalF3T1RFMU1qTXRORFl1TlRrd09EVTFNaXd4TURNdU5EQTVNVFV5TXkweE1ETXVOREE1TWpRd01YTXRORFl1TlRrd056VTBPUzB4TURNdU5EQTVNalF3TVMweE1ETXVOREE1TVRVeU15MHhNRE11TkRBNU1qSTNObG9pTHo0S0lDQWdJRHh3WVhSb0lHTnNZWE56UFNKemREQWlJR1E5SWswek56WXVNVE00TURVNU55d3lNVEl1Tnpnek5UZzNObWd0TkRrdU1UUTJOemMzTjNZM05DNDBNekk0TnpWb05Ea3VNVFEyTnpjM04yTXlNQzQxTlRRd01UVTFMREFzTXpjdU1qRTJORE0zTlMweE5pNDJOakl6TkRZM0xETTNMakl4TmpRek56VXRNemN1TWpFMk5ETTNOWFl0TGpBd01EQTNOVE5qTUMweU1DNDFOVFF3TVRVMUxURTJMalkyTWpReU1pMHpOeTR5TVRZek5qSXlMVE0zTGpJeE5qUXpOelV0TXpjdU1qRTJNell5TWxvaUx6NEtJQ0E4TDJjK0Nqd3ZjM1puUGc9PSIgYWx0PSIiIHdpZHRoPSIxNSIgaGVpZ2h0PSIxNSI+JywKICB0YWJfcHl0aG9uOiAnPHN2ZyBjbGFzcz0idGFiLWxvZ28gcHlsIiB2aWV3Qm94PSIwIDAgMjQgMjQiIGFyaWEtaGlkZGVuPSJ0cnVlIj48dGl0bGU+UHl0aG9uPC90aXRsZT48cGF0aCBkPSJNMTQuMjUuMThsLjkuMi43My4yNi41OS4zLjQ1LjMyLjM0LjM0LjI1LjM0LjE2LjMzLjEuMy4wNC4yNi4wMi4yLS4wMS4xM1Y4LjVsLS4wNS42My0uMTMuNTUtLjIxLjQ2LS4yNi4zOC0uMy4zMS0uMzMuMjUtLjM1LjE5LS4zNS4xNC0uMzMuMS0uMy4wNy0uMjYuMDQtLjIxLjAySDguNzdsLS42OS4wNS0uNTkuMTQtLjUuMjItLjQxLjI3LS4zMy4zMi0uMjcuMzUtLjIuMzYtLjE1LjM3LS4xLjM1LS4wNy4zMi0uMDQuMjctLjAyLjIxdjMuMDZIMy4xN2wtLjIxLS4wMy0uMjgtLjA3LS4zMi0uMTItLjM1LS4xOC0uMzYtLjI2LS4zNi0uMzYtLjM1LS40Ni0uMzItLjU5LS4yOC0uNzMtLjIxLS44OC0uMTQtMS4wNS0uMDUtMS4yMy4wNi0xLjIyLjE2LTEuMDQuMjQtLjg3LjMyLS43MS4zNi0uNTcuNC0uNDQuNDItLjMzLjQyLS4yNC40LS4xNi4zNi0uMS4zMi0uMDUuMjQtLjAxaC4xNmwuMDYuMDFoOC4xNnYtLjgzSDYuMThsLS4wMS0yLjc1LS4wMi0uMzcuMDUtLjM0LjExLS4zMS4xNy0uMjguMjUtLjI2LjMxLS4yMy4zOC0uMi40NC0uMTguNTEtLjE1LjU4LS4xMi42NC0uMS43MS0uMDYuNzctLjA0Ljg0LS4wMiAxLjI3LjA1em0tNi4zIDEuOThsLS4yMy4zMy0uMDguNDEuMDguNDEuMjMuMzQuMzMuMjIuNDEuMDkuNDEtLjA5LjMzLS4yMi4yMy0uMzQuMDgtLjQxLS4wOC0uNDEtLjIzLS4zMy0uMzMtLjIyLS40MS0uMDktLjQxLjA5em0xMy4wOSAzLjk1bC4yOC4wNi4zMi4xMi4zNS4xOC4zNi4yNy4zNi4zNS4zNS40Ny4zMi41OS4yOC43My4yMS44OC4xNCAxLjA0LjA1IDEuMjMtLjA2IDEuMjMtLjE2IDEuMDQtLjI0Ljg2LS4zMi43MS0uMzYuNTctLjQuNDUtLjQyLjMzLS40Mi4yNC0uNC4xNi0uMzYuMDktLjMyLjA1LS4yNC4wMi0uMTYtLjAxaC04LjIydi44Mmg1Ljg0bC4wMSAyLjc2LjAyLjM2LS4wNS4zNC0uMTEuMzEtLjE3LjI5LS4yNS4yNS0uMzEuMjQtLjM4LjItLjQ0LjE3LS41MS4xNS0uNTguMTMtLjY0LjA5LS43MS4wNy0uNzcuMDQtLjg0LjAxLTEuMjctLjA0LTEuMDctLjE0LS45LS4yLS43My0uMjUtLjU5LS4zLS40NS0uMzMtLjM0LS4zNC0uMjUtLjM0LS4xNi0uMzMtLjEtLjMtLjA0LS4yNS0uMDItLjIuMDEtLjEzdi01LjM0bC4wNS0uNjQuMTMtLjU0LjIxLS40Ni4yNi0uMzguMy0uMzIuMzMtLjI0LjM1LS4yLjM1LS4xNC4zMy0uMS4zLS4wNi4yNi0uMDQuMjEtLjAyLjEzLS4wMWg1Ljg0bC42OS0uMDUuNTktLjE0LjUtLjIxLjQxLS4yOC4zMy0uMzIuMjctLjM1LjItLjM2LjE1LS4zNi4xLS4zNS4wNy0uMzIuMDQtLjI4LjAyLS4yMVY2LjA3aDIuMDlsLjE0LjAxem0tNi40NyAxNC4yNWwtLjIzLjMzLS4wOC40MS4wOC40MS4yMy4zMy4zMy4yMy40MS4wOC40MS0uMDguMzMtLjIzLjIzLS4zMy4wOC0uNDEtLjA4LS40MS0uMjMtLjMzLS4zMy0uMjMtLjQxLS4wOC0uNDEuMDh6Ii8+PC9zdmc+JywKICB0YWJfbm9kZTogJzxzdmcgY2xhc3M9InRhYi1sb2dvIG5kbCIgdmlld0JveD0iMCAwIDI0IDI0IiBhcmlhLWhpZGRlbj0idHJ1ZSI+PHRpdGxlPk5vZGUuanM8L3RpdGxlPjxwYXRoIGQ9Ik0xMS45OTgsMjRjLTAuMzIxLDAtMC42NDEtMC4wODQtMC45MjItMC4yNDdsLTIuOTM2LTEuNzM3Yy0wLjQzOC0wLjI0NS0wLjIyNC0wLjMzMi0wLjA4LTAuMzgzIGMwLjU4NS0wLjIwMywwLjcwMy0wLjI1LDEuMzI4LTAuNjA0YzAuMDY1LTAuMDM3LDAuMTUxLTAuMDIzLDAuMjE4LDAuMDE3bDIuMjU2LDEuMzM5YzAuMDgyLDAuMDQ1LDAuMTk3LDAuMDQ1LDAuMjcyLDBsOC43OTUtNS4wNzYgYzAuMDgyLTAuMDQ3LDAuMTM0LTAuMTQxLDAuMTM0LTAuMjM4VjYuOTIxYzAtMC4wOTktMC4wNTMtMC4xOTItMC4xMzctMC4yNDJsLTguNzkxLTUuMDcyYy0wLjA4MS0wLjA0Ny0wLjE4OS0wLjA0Ny0wLjI3MSwwIEwzLjA3NSw2LjY4QzIuOTksNi43MjksMi45MzYsNi44MjUsMi45MzYsNi45MjF2MTAuMTVjMCwwLjA5NywwLjA1NCwwLjE4OSwwLjEzOSwwLjIzNWwyLjQwOSwxLjM5MiBjMS4zMDcsMC42NTQsMi4xMDgtMC4xMTYsMi4xMDgtMC44OVY3Ljc4N2MwLTAuMTQyLDAuMTE0LTAuMjUzLDAuMjU2LTAuMjUzaDEuMTE1YzAuMTM5LDAsMC4yNTUsMC4xMTIsMC4yNTUsMC4yNTN2MTAuMDIxIGMwLDEuNzQ1LTAuOTUsMi43NDUtMi42MDQsMi43NDVjLTAuNTA4LDAtMC45MDksMC0yLjAyNi0wLjU1MUwyLjI4LDE4LjY3NWMtMC41Ny0wLjMyOS0wLjkyMi0wLjk0NS0wLjkyMi0xLjYwNFY2LjkyMSBjMC0wLjY1OSwwLjM1My0xLjI3NSwwLjkyMi0xLjYwM2w4Ljc5NS01LjA4MmMwLjU1Ny0wLjMxNSwxLjI5Ni0wLjMxNSwxLjg0OCwwbDguNzk0LDUuMDgyYzAuNTcsMC4zMjksMC45MjQsMC45NDQsMC45MjQsMS42MDMgdjEwLjE1YzAsMC42NTktMC4zNTQsMS4yNzMtMC45MjQsMS42MDRsLTguNzk0LDUuMDc4QzEyLjY0MywyMy45MTYsMTIuMzI0LDI0LDExLjk5OCwyNHogTTE5LjA5OSwxMy45OTMgYzAtMS45LTEuMjg0LTIuNDA2LTMuOTg3LTIuNzYzYy0yLjczMS0wLjM2MS0zLjAwOS0wLjU0OC0zLjAwOS0xLjE4N2MwLTAuNTI4LDAuMjM1LTEuMjMzLDIuMjU4LTEuMjMzIGMxLjgwNywwLDIuNDczLDAuMzg5LDIuNzQ3LDEuNjA3YzAuMDI0LDAuMTE1LDAuMTI5LDAuMTk5LDAuMjQ3LDAuMTk5aDEuMTQxYzAuMDcxLDAsMC4xMzgtMC4wMzEsMC4xODYtMC4wODEgYzAuMDQ4LTAuMDU0LDAuMDc0LTAuMTIzLDAuMDY3LTAuMTk2Yy0wLjE3Ny0yLjA5OC0xLjU3MS0zLjA3Ni00LjM4OC0zLjA3NmMtMi41MDgsMC00LjAwNCwxLjA1OC00LjAwNCwyLjgzMyBjMCwxLjkyNSwxLjQ4OCwyLjQ1NywzLjg5NSwyLjY5NWMyLjg4LDAuMjgyLDMuMTAzLDAuNzAzLDMuMTAzLDEuMjY5YzAsMC45ODMtMC43ODksMS40MDItMi42NDIsMS40MDIgYy0yLjMyNywwLTIuODM5LTAuNTg0LTMuMDExLTEuNzQyYy0wLjAyLTAuMTI0LTAuMTI2LTAuMjE1LTAuMjUzLTAuMjE1aC0xLjEzN2MtMC4xNDEsMC0wLjI1NCwwLjExMi0wLjI1NCwwLjI1MyBjMCwxLjQ4MiwwLjgwNiwzLjI0OCw0LjY1NSwzLjI0OEMxNy41MDEsMTcuMDA3LDE5LjA5OSwxNS45MSwxOS4wOTksMTMuOTkzeiIvPjwvc3ZnPicKfTsKZnVuY3Rpb24gYXR0YWNoUGFyZW4oYywgcGlubmVkKXsKICBjb25zdCBsb2MgPSAiVFlQRSB2Z2ksIExPQ0FUSU9OICciK3NlcnZpY2VVcmwrIiciOwogIHJldHVybiBwaW5uZWQgPyBsb2MrIiwgREFUQV9WRVJTSU9OICciK3Bpbm5lZCsiJyIgOiBsb2M7Cn0KZnVuY3Rpb24gZXhhbXBsZVRhYmxlKGMpewogIGNvbnN0IHMgPSBjLnNjaGVtYXNbMF07IGNvbnN0IHQgPSAocy50YWJsZXNbMF18fHMudmlld3NbMF18fHtuYW1lOidteV90YWJsZSd9KTsKICByZXR1cm4gcy5uYW1lKycuJyt0Lm5hbWU7Cn0KZnVuY3Rpb24gY29ubmVjdENvZGUoYywgcGlubmVkLCBsYW5nKXsKICBjb25zdCBxPWMubmFtZSwgZXg9ZXhhbXBsZVRhYmxlKGMpLCBQPWF0dGFjaFBhcmVuKGMscGlubmVkKTsKICBpZiAobGFuZz09PSdzcWwnKSByZXR1cm4gIklOU1RBTEwgdmdpIEZST00gY29tbXVuaXR5O1xuTE9BRCB2Z2k7XG4iICsKICAgICJBVFRBQ0ggJyIrcSsiJyBBUyAiK3ErIlxuICAoIitQKyIpO1xuXG4iICsKICAgICJTRUxFQ1QgKiBGUk9NICIrcSsiLiIrZXgrIiBMSU1JVCAxMDsiOwogIGlmIChsYW5nPT09J3B5dGhvbicpIHJldHVybiAiaW1wb3J0IGhheWJhcm4gYXMgZHVja2RiXG5cbiIgKwogICAgImNvbiA9IGR1Y2tkYi5jb25uZWN0KClcbiIgKwogICAgImNvbi5leGVjdXRlKFwiSU5TVEFMTCB2Z2kgRlJPTSBjb21tdW5pdHlcIilcbiIgKwogICAgImNvbi5leGVjdXRlKFwiTE9BRCB2Z2lcIilcbiIgKwogICAgImNvbi5leGVjdXRlKFwiQVRUQUNIICciK3ErIicgQVMgIitxKyIgKCIrUCsiKVwiKVxuIiArCiAgICAiY29uLnNxbChcIlNFTEVDVCAqIEZST00gIitxKyIuIitleCsiIExJTUlUIDEwXCIpLnNob3coKSI7CiAgcmV0dXJuICJpbXBvcnQgeyBEdWNrREJJbnN0YW5jZSB9IGZyb20gXCJAaGF5YmFybi9ub2RlLWFwaVwiO1xuXG4iICsKICAgICJjb25zdCBkYiA9IGF3YWl0IER1Y2tEQkluc3RhbmNlLmNyZWF0ZSgpO1xuIiArCiAgICAiY29uc3QgY29uID0gYXdhaXQgZGIuY29ubmVjdCgpO1xuIiArCiAgICAiYXdhaXQgY29uLnJ1bihcIklOU1RBTEwgdmdpIEZST00gY29tbXVuaXR5XCIpO1xuIiArCiAgICAiYXdhaXQgY29uLnJ1bihcIkxPQUQgdmdpXCIpO1xuIiArCiAgICAiYXdhaXQgY29uLnJ1bihcIkFUVEFDSCAnIitxKyInIEFTICIrcSsiICgiK1ArIilcIik7XG4iICsKICAgICJjb25zdCByZXN1bHQgPSBhd2FpdCBjb24ucnVuQW5kUmVhZEFsbChcIlNFTEVDVCAqIEZST00gIitxKyIuIitleCsiIExJTUlUIDEwXCIpO1xuIiArCiAgICAiY29uc29sZS50YWJsZShyZXN1bHQuZ2V0Um93cygpKTsiOwp9CmZ1bmN0aW9uIGhsQ29kZShjb2RlLCBsYW5nKXsKICBsZXQgaCA9IGVzYyhjb2RlKTsKICBoID0gaC5yZXBsYWNlKC8mcXVvdDsoW14mXSopJnF1b3Q7L2csICc8c3BhbiBjbGFzcz0ic3RyIj4mcXVvdDskMSZxdW90Ozwvc3Bhbj4nKTsKICBoID0gaC5yZXBsYWNlKC8nKFteJ10qKScvZywgJzxzcGFuIGNsYXNzPSJzdHIiPiYjMzk7JDEmIzM5Ozwvc3Bhbj4nKTsKICBjb25zdCBLVyA9IHsgc3FsOlsnSU5TVEFMTCcsJ0xPQUQnLCdBVFRBQ0gnLCdBUycsJ1RZUEUnLCdMT0NBVElPTicsJ0RBVEFfVkVSU0lPTicsJ1NFTEVDVCcsJ0ZST00nLCdMSU1JVCddLCBweXRob246WydpbXBvcnQnXSwganM6WydpbXBvcnQnLCdjb25zdCcsJ2F3YWl0JywnbmV3J10gfTsKICAoS1dbbGFuZ118fFtdKS5mb3JFYWNoKGs9PiBoID0gaC5yZXBsYWNlKG5ldyBSZWdFeHAoJ1xcYicraysnXFxiJywnZycpLCAnPHNwYW4gY2xhc3M9Imt3Ij4nK2srJzwvc3Bhbj4nKSk7CiAgcmV0dXJuIGg7Cn0KZnVuY3Rpb24gYXR0YWNoT3B0aW9uc0h0bWwoYyl7CiAgY29uc3Qgb3B0cyA9IGMuYXR0YWNoX29wdGlvbnN8fFtdOwogIGlmKCFvcHRzLmxlbmd0aCkgcmV0dXJuICcnOwogIHJldHVybiAnPGRldGFpbHMgY2xhc3M9ImF0dGFjaC1vcHRzIj48c3VtbWFyeSBjbGFzcz0iYW8tc3VtbWFyeSI+JytpY29uKCdjYXJldCcsJ3RyaScpKwogICAgJzxzcGFuIGNsYXNzPSJhcmdzLWxhYmVsIiBzdHlsZT0ibWFyZ2luOjAiPkF0dGFjaCBvcHRpb25zPC9zcGFuPjxzcGFuIGNsYXNzPSJhby1jb3VudCI+JytvcHRzLmxlbmd0aCsnPC9zcGFuPjwvc3VtbWFyeT4nKwogICAgJzx0YWJsZSBjbGFzcz0ib3B0cyI+PHRoZWFkPjx0cj48dGg+T3B0aW9uPC90aD48dGg+VHlwZTwvdGg+PHRoPkRlZmF1bHQ8L3RoPjx0aD5EZXNjcmlwdGlvbjwvdGg+PC90cj48L3RoZWFkPjx0Ym9keT4nKwogICAgb3B0cy5tYXAobz0+Jzx0cj48dGQgY2xhc3M9Im8tbmFtZSI+Jytlc2Moby5uYW1lKSsnPC90ZD48dGQgY2xhc3M9Im8tdHlwZSI+Jytlc2Moby50eXBlKSsnPC90ZD48dGQgY2xhc3M9Im8tZGVmIj4nK2VzYyhvLmRlZmF1bHR8fCfigJQnKSsnPC90ZD48dGQgY2xhc3M9Im8tZGVzYyI+Jytlc2Moby5kZXNjcmlwdGlvbnx8JycpKyc8L3RkPjwvdHI+Jykuam9pbignJykrCiAgICAnPC90Ym9keT48L3RhYmxlPjwvZGV0YWlscz4nOwp9CmZ1bmN0aW9uIHJlbmRlckNvbm5lY3QoKXsKICBjb25zdCBjID0gY2F0KCk7CiAgY29uc3QgcGlubmVkID0gY3VycmVudFNwZWMoYyk7CiAgY29uc3QgbGFuZyA9IENPTk5FQ1RfVEFCU1tjb25uZWN0VGFiXVswXTsKICBjb25zdCBjb2RlID0gY29ubmVjdENvZGUoYywgcGlubmVkLCBsYW5nKTsKICBjb25zdCB0YWJzID0gQ09OTkVDVF9UQUJTLm1hcCgoW2ssbGJsLGljXSxpKT0+JzxidXR0b24gY2xhc3M9InRhYicrKGk9PT1jb25uZWN0VGFiPycgYWN0aXZlJzonJykrJyIgZGF0YS1pPSInK2krJyI+JysoVEFCSUNPTlNbaWNdfHwnJykrZXNjKGxibCkrJzwvYnV0dG9uPicpLmpvaW4oJycpOwogIGxldCBkdiA9ICcnOwogIGlmIChjLmRhdGFfdmVyc2lvbnMgJiYgYy5kYXRhX3ZlcnNpb25zLmxlbmd0aCl7CiAgICBjb25zdCBjdXJMYWJlbCA9IHBpbm5lZCA/IHBpbm5lZCA6ICdsYXRlc3QgwrcgJytsYXRlc3RTcGVjKGMpOwogICAgY29uc3QgaXRlbXMgPSBbJzxidXR0b24gY2xhc3M9ImR2aXRlbScrKCFwaW5uZWQ/JyBzZWwnOicnKSsnIiBkYXRhLXNwZWM9IiI+JysKICAgICAgICAnPHNwYW4gY2xhc3M9InJvdzEiPjxzcGFuIGNsYXNzPSJzcGVjIj5sYXRlc3Q8L3NwYW4+PHNwYW4gY2xhc3M9InRhZyI+dHJhY2tpbmc8L3NwYW4+JysoIXBpbm5lZD8nPHNwYW4gY2xhc3M9ImNoZWNrIj7inJM8L3NwYW4+JzonJykrJzwvc3Bhbj4nKwogICAgICAgICc8c3BhbiBjbGFzcz0ibGJsIj5BbHdheXMgdGhlIG5ld2VzdCB2ZXJzaW9uICgnK2VzYyhsYXRlc3RTcGVjKGMpKSsnKTwvc3Bhbj48L2J1dHRvbj4nXQogICAgICAuY29uY2F0KGMuZGF0YV92ZXJzaW9ucy5tYXAoKHYsaSk9PgogICAgICAgICc8YnV0dG9uIGNsYXNzPSJkdml0ZW0nKyhwaW5uZWQ9PT12LnNwZWM/JyBzZWwnOicnKSsnIiBkYXRhLXNwZWM9IicrZXNjKHYuc3BlYykrJyI+JysKICAgICAgICAgICc8c3BhbiBjbGFzcz0icm93MSI+PHNwYW4gY2xhc3M9InNwZWMiPicrZXNjKHYuc3BlYykrJzwvc3Bhbj4nKwogICAgICAgICAgKGk9PT0wPyc8c3BhbiBjbGFzcz0idGFnIj5sYXRlc3Q8L3NwYW4+JzonPHNwYW4gY2xhc3M9InRhZyBwaW4iPnBpbm5lZDwvc3Bhbj4nKSsKICAgICAgICAgIChwaW5uZWQ9PT12LnNwZWM/JzxzcGFuIGNsYXNzPSJjaGVjayI+4pyTPC9zcGFuPic6JycpKyc8L3NwYW4+JysKICAgICAgICAgICh2LmxhYmVsPyc8c3BhbiBjbGFzcz0ibGJsIj4nK2VzYyh2LmxhYmVsKSsnPC9zcGFuPic6JycpKyc8L2J1dHRvbj4nKSkuam9pbignJyk7CiAgICBkdiA9ICc8ZGV0YWlscyBjbGFzcz0iZHZtZW51IiBpZD0iZHZtZW51Ij48c3VtbWFyeT5EYXRhIHZlcnNpb246IDxzcGFuIGNsYXNzPSJkdi1jdXIiPicrZXNjKGN1ckxhYmVsKSsnPC9zcGFuPiA8c3BhbiBjbGFzcz0iY2FyZXQiPuKWvjwvc3Bhbj48L3N1bW1hcnk+JysKICAgICAgICAgJzxkaXYgY2xhc3M9ImR2bGlzdCI+JytpdGVtcysnPC9kaXY+PC9kZXRhaWxzPic7CiAgfQogIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjb25uZWN0JykuaW5uZXJIVE1MID0KICAgICc8ZGl2IGNsYXNzPSJjb25uZWN0LXRvcCI+PGRpdiBjbGFzcz0idGFicyI+Jyt0YWJzKyc8L2Rpdj4nK2R2Kyc8L2Rpdj4nKwogICAgJzxkaXYgY2xhc3M9ImF0dGFjaCI+PGJ1dHRvbiBjbGFzcz0iY29weSIgaWQ9ImNvcHktYnRuIiBhcmlhLWxhYmVsPSJDb3B5IGNvZGUiPkNvcHk8L2J1dHRvbj4nK2hsQ29kZShjb2RlLCBsYW5nKSsnPC9kaXY+JysKICAgIGF0dGFjaE9wdGlvbnNIdG1sKGMpOwogIGNvbnN0IGNwID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2NvcHktYnRuJyk7CiAgY3AuYWRkRXZlbnRMaXN0ZW5lcignY2xpY2snLCAoKT0+ewogICAgY29uc3QgZG9uZT0oKT0+eyBjcC50ZXh0Q29udGVudD0nQ29waWVkJzsgc2V0VGltZW91dCgoKT0+Y3AudGV4dENvbnRlbnQ9J0NvcHknLDE1MDApOyB9OwogICAgaWYgKG5hdmlnYXRvci5jbGlwYm9hcmQpIG5hdmlnYXRvci5jbGlwYm9hcmQud3JpdGVUZXh0KGNvZGUpLnRoZW4oZG9uZSxkb25lKTsKICAgIGVsc2UgeyBjb25zdCB0PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3RleHRhcmVhJyk7IHQudmFsdWU9Y29kZTsgZG9jdW1lbnQuYm9keS5hcHBlbmRDaGlsZCh0KTsgdC5zZWxlY3QoKTsgdHJ5e2RvY3VtZW50LmV4ZWNDb21tYW5kKCdjb3B5Jyk7fWNhdGNoKGUpe30gdC5yZW1vdmUoKTsgZG9uZSgpOyB9CiAgfSk7CiAgZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbCgnI2Nvbm5lY3QgLnRhYicpLmZvckVhY2goYj0+Yi5hZGRFdmVudExpc3RlbmVyKCdjbGljaycsKCk9PnsgY29ubmVjdFRhYj0rYi5kYXRhc2V0Lmk7IHJlbmRlckNvbm5lY3QoKTsgfSkpOwogIGNvbnN0IG1lbnUgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZHZtZW51Jyk7CiAgaWYgKG1lbnUpIG1lbnUucXVlcnlTZWxlY3RvckFsbCgnLmR2aXRlbScpLmZvckVhY2goYj0+Yi5hZGRFdmVudExpc3RlbmVyKCdjbGljaycsKCk9PnsKICAgIGNvbnN0IHNwZWM9Yi5kYXRhc2V0LnNwZWM7IGlmKHNwZWMpIHNlbGVjdGVkW2MubmFtZV09c3BlYzsgZWxzZSBkZWxldGUgc2VsZWN0ZWRbYy5uYW1lXTsKICAgIHJlbmRlckNvbm5lY3QoKTsgdXBkYXRlQ3Vwb2xhKCk7IHJlbmRlckNvbnRlbnRzKCk7CiAgfSkpOwp9CmRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoJ2NsaWNrJywgZT0+ewogIGNvbnN0IG1lbnUgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZHZtZW51Jyk7CiAgaWYgKG1lbnUgJiYgbWVudS5vcGVuICYmICFtZW51LmNvbnRhaW5zKGUudGFyZ2V0KSkgbWVudS5vcGVuPWZhbHNlOwp9KTsKZnVuY3Rpb24gdXBkYXRlQ3Vwb2xhKCl7IGNvbnN0IHUgPSBjdXBvbGFVcmwoIiIpOyBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnY3Vwb2xhLWN0YScpLmhyZWYgPSB1OyBjb25zdCBhaSA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjdXBvbGEtYWktY3RhJyk7IGlmIChhaSkgYWkuaHJlZiA9IHU7IH0KCi8vIOKUgOKUgCBvYmplY3Qgcm93cyDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKZnVuY3Rpb24gdGFibGVSb3coY2F0TmFtZSwgcywgdCwga2luZCl7ICAgICAgIC8vIGtpbmQ6ICd0YWJsZScgfCAndmlldycKICBjb25zdCBpY29uTmFtZSA9IGtpbmQ9PT0ndmlldycgPyAndmlldycgOiAndGFibGUnOwogIGNvbnN0IGtjbHMgPSBraW5kPT09J3ZpZXcnID8gJyB2aWV3IGstdmlldycgOiAnJzsKICByZXR1cm4gJzxkZXRhaWxzIGNsYXNzPSJ0YmwnK2tjbHMrJyIgZGF0YS1uYW1lPSInK2VzYyh0Lm5hbWUudG9Mb3dlckNhc2UoKSkrJyIgZGF0YS1raW5kPSInK2tpbmQrJyIgZGF0YS1jYXRhbG9nPSInK2VzYyhjYXROYW1lKSsnIiBkYXRhLXNjaGVtYT0iJytlc2Mocy5uYW1lKSsnIiBkYXRhLXRhYmxlPSInK2VzYyh0Lm5hbWUpKyciPicrCiAgICAnPHN1bW1hcnk+JytpY29uKCdjYXJldCcsJ3RyaScpK2ljb24oaWNvbk5hbWUsJ3Jvdy1pY29uJykrCiAgICAgICc8c3BhbiBjbGFzcz0idC1uYW1lIj4nK2VzYyh0Lm5hbWUpKyc8L3NwYW4+PHNwYW4gY2xhc3M9InQtbWV0YSI+Jyt0LmNvbHMrJyBjb2xzPC9zcGFuPjwvc3VtbWFyeT4nKwogICAgJzxkaXYgY2xhc3M9InQtYm9keSI+JysKICAgICAgKHQuY29tbWVudCA/ICc8ZGl2IGNsYXNzPSJ0LWRlc2MiPicrbWRUb0h0bWwodC5jb21tZW50KSsnPC9kaXY+JyA6ICcnKSsKICAgICAgKGtpbmQ9PT0ndmlldycgJiYgdC5kZWYgPyAnPGRpdiBjbGFzcz0iYXJncy1sYWJlbCI+RGVmaW5pdGlvbjwvZGl2PjxwcmUgY2xhc3M9InZpZXctc3FsIj4nK3NxbEhpKHQuZGVmKSsnPC9wcmU+JyA6ICcnKSsKICAgICAgJzxkaXYgY2xhc3M9ImFyZ3MtbGFiZWwiPkNvbHVtbnM8L2Rpdj48ZGl2IGNsYXNzPSJjb2xzIj48ZGl2IGNsYXNzPSJza2VsZXRvbiI+TG9hZGluZyBjb2x1bW5z4oCmPC9kaXY+PC9kaXY+PC9kaXY+PC9kZXRhaWxzPic7Cn0KCmNvbnN0IEZOX1RZUEVTID0gW1snc2NhbGFyJywnU2NhbGFyIGZ1bmN0aW9ucycsJ2Z1bmN0aW9uJywnay1zY2FsYXInXSwKICAgICAgICAgICAgICAgICAgWyd0YWJsZScsJ1RhYmxlIGZ1bmN0aW9ucycsJ2Z1bmN0aW9uJywnay10YWJsZWZuJ10sCiAgICAgICAgICAgICAgICAgIFsnYWdncmVnYXRlJywnQWdncmVnYXRlIGZ1bmN0aW9ucycsJ2FnZ3JlZ2F0ZScsJ2stYWdnJ10sCiAgICAgICAgICAgICAgICAgIFsndGFibGVfaW5fb3V0JywnVGFibGUgaW4tb3V0IGZ1bmN0aW9ucycsJ3RpbycsJ2stdGlvJ11dOwpmdW5jdGlvbiBhcmdDYXJkKGEpewogIGNvbnN0IGRlZiA9IChhLmRlZmF1bHQhPT11bmRlZmluZWQgJiYgYS5kZWZhdWx0IT09bnVsbCkgPyAnPHNwYW4gY2xhc3M9ImFyZy1kZWYiPmRlZmF1bHQgJytlc2MoYS5kZWZhdWx0KSsnPC9zcGFuPicgOiAnJzsKICBjb25zdCBkZXNjID0gYS5kZXNjID8gZXNjKGEuZGVzYykgOiAnJzsKICBjb25zdCBtZXRhID0gKGRlc2N8fGRlZikgPyAnPGRpdiBjbGFzcz0iY29sLWNvbW1lbnQiPicrZGVzYysoZGVzYyYmZGVmPycgwrcgJzonJykrZGVmKyc8L2Rpdj4nIDogJyc7CiAgcmV0dXJuICc8ZGl2IGNsYXNzPSJjb2wtaXRlbSI+PGRpdiBjbGFzcz0iY29sIj48c3BhbiBjbGFzcz0iY29sLW5hbWUiPicrZXNjKGEubmFtZSkrKGEubmFtZWQ/JzxzcGFuIGNsYXNzPSJhcmcta2luZCI+IDo9PC9zcGFuPic6JycpKyc8L3NwYW4+PHNwYW4gY2xhc3M9ImNvbC10eXBlIj4nK2VzYyhkdWNrZGJUeXBlKGEudHlwZSkpKyc8L3NwYW4+PC9kaXY+JyttZXRhKyc8L2Rpdj4nOwp9CmZ1bmN0aW9uIGZuUm93KGYsIGljb25OYW1lKXsKICBjb25zdCBwb3MgPSBmLmFyZ3MuZmlsdGVyKGE9PiFhLm5hbWVkKSwgbmFtZWQgPSBmLmFyZ3MuZmlsdGVyKGE9PmEubmFtZWQpOwogIGNvbnN0IHNpZyA9ICcoJysgcG9zLm1hcChhPT5hLm5hbWUpLmNvbmNhdChuYW1lZC5tYXAoYT0+YS5uYW1lKycgOj0nKSkuam9pbignLCAnKSArJyknOwogIGxldCBhcmdzR3JpZDsKICBpZiAoIWYuYXJncy5sZW5ndGgpIGFyZ3NHcmlkID0gJzxkaXYgY2xhc3M9ImZuLW5vYXJncyI+Tm8gYXJndW1lbnRzPC9kaXY+JzsKICBlbHNlIGlmICghbmFtZWQubGVuZ3RoKSBhcmdzR3JpZCA9ICc8ZGl2IGNsYXNzPSJhcmdzLWxhYmVsIj5Bcmd1bWVudHM8L2Rpdj48ZGl2IGNsYXNzPSJjb2xzIj4nK3Bvcy5tYXAoYXJnQ2FyZCkuam9pbignJykrJzwvZGl2Pic7CiAgZWxzZSBhcmdzR3JpZCA9IChwb3MubGVuZ3RoID8gJzxkaXYgY2xhc3M9ImFyZ3MtbGFiZWwiPlBvc2l0aW9uYWwgYXJndW1lbnRzPC9kaXY+PGRpdiBjbGFzcz0iY29scyI+Jytwb3MubWFwKGFyZ0NhcmQpLmpvaW4oJycpKyc8L2Rpdj4nIDogJycpKwogICAgICAgICAgICAgICAgICAnPGRpdiBjbGFzcz0iYXJncy1sYWJlbCIgc3R5bGU9Im1hcmdpbi10b3A6MTJweCI+TmFtZWQgYXJndW1lbnRzPC9kaXY+PGRpdiBjbGFzcz0iY29scyI+JytuYW1lZC5tYXAoYXJnQ2FyZCkuam9pbignJykrJzwvZGl2Pic7CiAgY29uc3QgcmV0ID0gZi5yZXR1cm5zID8gJzxkaXYgY2xhc3M9ImZuLXJldHVybnMiPjxiPlJldHVybnM8L2I+PHNwYW4gY2xhc3M9InJ0eXBlIj4nK2VzYyhmLnJldHVybnMpKyc8L3NwYW4+PC9kaXY+JyA6ICcnOwogIHJldHVybiAnPGRldGFpbHMgY2xhc3M9ImZuIiBkYXRhLW5hbWU9IicrZXNjKGYubmFtZS50b0xvd2VyQ2FzZSgpKSsnIj4nKwogICAgJzxzdW1tYXJ5PicraWNvbignY2FyZXQnLCd0cmknKStpY29uKGljb25OYW1lLCdyb3ctaWNvbicpKyc8c3BhbiBjbGFzcz0iZm4tbmFtZSI+Jytlc2MoZi5uYW1lKSsnPC9zcGFuPjxzcGFuIGNsYXNzPSJmbi1zaWciPicrZXNjKHNpZykrJzwvc3Bhbj48L3N1bW1hcnk+JysKICAgICc8ZGl2IGNsYXNzPSJmbi1ib2R5Ij4nKyhmLmRvYz8gJzxkaXYgY2xhc3M9InQtZGVzYyI+JyttZFRvSHRtbChmLmRvYykrJzwvZGl2Pic6JycpK2FyZ3NHcmlkK3JldCsnPC9kaXY+PC9kZXRhaWxzPic7Cn0KZnVuY3Rpb24gZm5Hcm91cHMoZm5zKXsKICBpZiAoIWZucy5sZW5ndGgpIHJldHVybiAnJzsKICBjb25zdCBrbm93biA9IEZOX1RZUEVTLm1hcCh4PT54WzBdKTsKICBsZXQgb3V0ID0gJyc7CiAgRk5fVFlQRVMuZm9yRWFjaCgoW3QsbGJsLGljLGNsc10pPT57CiAgICBjb25zdCBnID0gZm5zLmZpbHRlcihmPT5mLnR5cGU9PT10KTsKICAgIGlmICghZy5sZW5ndGgpIHJldHVybjsKICAgIG91dCArPSAnPGRpdiBjbGFzcz0iZ3JwICcrY2xzKyciPjxkaXYgY2xhc3M9ImdsYWJlbCI+JytpY29uKGljKStsYmwrJzwvZGl2PicgKyBnLm1hcChmPT5mblJvdyhmLGljKSkuam9pbignJykgKyAnPC9kaXY+JzsKICB9KTsKICBjb25zdCBvdGhlciA9IGZucy5maWx0ZXIoZj0+IWtub3duLmluY2x1ZGVzKGYudHlwZSkpOwogIGlmIChvdGhlci5sZW5ndGgpIG91dCArPSAnPGRpdiBjbGFzcz0iZ3JwIj48ZGl2IGNsYXNzPSJnbGFiZWwiPicraWNvbignZnVuY3Rpb24nKSsnT3RoZXIgZnVuY3Rpb25zPC9kaXY+JytvdGhlci5tYXAoZj0+Zm5Sb3coZiwnZnVuY3Rpb24nKSkuam9pbignJykrJzwvZGl2Pic7CiAgcmV0dXJuIG91dDsKfQoKLy8g4pSA4pSAIGNvbnRlbnRzIOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgOKUgApmdW5jdGlvbiByZW5kZXJDb250ZW50cygpewogIGNvbnN0IGMgPSBjYXQoKSwgcm9vdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjYXRhbG9ncycpLCBjYyA9IGMuY291bnRzOwogIGNvbnN0IGFzb2YgPSAoYy5kYXRhX3ZlcnNpb25zICYmIGMuZGF0YV92ZXJzaW9ucy5sZW5ndGgpCiAgICA/ICcgwrcgPHNwYW4gY2xhc3M9ImFzb2YiPmFzIG9mICcrZXNjKGN1cnJlbnRTcGVjKGMpIHx8ICdsYXRlc3QgKCcrbGF0ZXN0U3BlYyhjKSsnKScpKyc8L3NwYW4+JyA6ICcnOwogIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjYXQtc3ViJykuaW5uZXJIVE1MID0KICAgIGNjLnNjaGVtYXMrJyBzY2hlbWFzIMK3ICcrY2MudGFibGVzKycgdGFibGVzIMK3ICcrY2Mudmlld3MrJyB2aWV3cyDCtyAnK2NjLmZ1bmN0aW9ucysnIGZ1bmN0aW9ucycrYXNvZjsKCiAgY29uc3Qgc2luZ2xlID0gYy5zY2hlbWFzLmxlbmd0aD09PTE7CiAgcm9vdC5pbm5lckhUTUwgPSBjLnNjaGVtYXMubWFwKHM9PnsKICAgIGNvbnN0IHN1YiA9IFsgcy50YWJsZXMubGVuZ3RoKycgdGFibGUnKyhzLnRhYmxlcy5sZW5ndGghPT0xPydzJzonJyksCiAgICAgICAgICAgICAgICAgIHMudmlld3MubGVuZ3RoPyBzLnZpZXdzLmxlbmd0aCsnIHZpZXcnKyhzLnZpZXdzLmxlbmd0aCE9PTE/J3MnOicnKTpudWxsLAogICAgICAgICAgICAgICAgICBzLmZ1bmN0aW9ucy5sZW5ndGg/IHMuZnVuY3Rpb25zLmxlbmd0aCsnIGZ1bmN0aW9uJysocy5mdW5jdGlvbnMubGVuZ3RoIT09MT8ncyc6JycpOm51bGwKICAgICAgICAgICAgICAgIF0uZmlsdGVyKEJvb2xlYW4pLmpvaW4oJyDCtyAnKTsKICAgIGNvbnN0IG9wZW5BdHRyID0gKHNpbmdsZSAmJiBzLnRhYmxlcy5sZW5ndGg8PTgpID8gJyBvcGVuJyA6ICcnOwogICAgY29uc3QgdGJsR3JwID0gcy50YWJsZXMubGVuZ3RoID8gJzxkaXYgY2xhc3M9ImdycCBrLXRhYmxlIj48ZGl2IGNsYXNzPSJnbGFiZWwiPicraWNvbigndGFibGUnKSsnVGFibGVzPC9kaXY+JysKICAgICAgcy50YWJsZXMubWFwKHQ9PnRhYmxlUm93KGMubmFtZSxzLHQsJ3RhYmxlJykpLmpvaW4oJycpKyc8L2Rpdj4nIDogJyc7CiAgICBjb25zdCB2aWV3R3JwID0gcy52aWV3cy5sZW5ndGggPyAnPGRpdiBjbGFzcz0iZ3JwIGstdmlldyI+PGRpdiBjbGFzcz0iZ2xhYmVsIj4nK2ljb24oJ3ZpZXcnKSsnVmlld3M8L2Rpdj4nKwogICAgICBzLnZpZXdzLm1hcCh2PT50YWJsZVJvdyhjLm5hbWUscyx2LCd2aWV3JykpLmpvaW4oJycpKyc8L2Rpdj4nIDogJyc7CiAgICBjb25zdCBmbkdycCA9IGZuR3JvdXBzKHMuZnVuY3Rpb25zKTsKICAgIGNvbnN0IHNkZXNjID0gcy5kb2MgPyAnPGRpdiBjbGFzcz0idC1kZXNjIHNjaGVtYS1kZXNjIj4nK21kVG9IdG1sKHMuZG9jKSsnPC9kaXY+JyA6ICcnOwogICAgcmV0dXJuICc8ZGV0YWlscyBjbGFzcz0ic2NoZW1hIiBkYXRhLW5hbWU9IicrZXNjKHMubmFtZS50b0xvd2VyQ2FzZSgpKSsnIicrb3BlbkF0dHIrJz4nKwogICAgICAnPHN1bW1hcnk+JytpY29uKCdjYXJldCcsJ3RyaScpK2ljb24oJ3NjaGVtYScsJ3R5cGUnKSsKICAgICAgICAnPHNwYW4gY2xhc3M9InNjaGVtYS1uYW1lIj4nK2VzYyhzLm5hbWUpKyc8L3NwYW4+PHNwYW4gY2xhc3M9InNjaGVtYS1zdWIiPicrc3ViKyc8L3NwYW4+PC9zdW1tYXJ5PicrCiAgICAgIHNkZXNjK3RibEdycCt2aWV3R3JwK2ZuR3JwKyc8L2RldGFpbHM+JzsKICB9KS5qb2luKCcnKTsKICBjb25zdCBmID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2ZpbHRlcicpOyBpZiAoZi52YWx1ZSkgYXBwbHlGaWx0ZXIoKTsKfQoKLy8gbGF6eS1sb2FkIGNvbHVtbnMgb24gZmlyc3QgdGFibGUvdmlldyBleHBhbmQKZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2NhdGFsb2dzJykuYWRkRXZlbnRMaXN0ZW5lcigndG9nZ2xlJywgZT0+ewogIGNvbnN0IGQgPSBlLnRhcmdldDsKICBpZiAoIWQuY2xhc3NMaXN0IHx8ICFkLmNsYXNzTGlzdC5jb250YWlucygndGJsJykgfHwgIWQub3BlbiB8fCBkLmRhdGFzZXQubG9hZGVkKSByZXR1cm47CiAgZC5kYXRhc2V0LmxvYWRlZCA9ICcxJzsKICBjb25zdCBib2R5ID0gZC5xdWVyeVNlbGVjdG9yKCcuY29scycpOwogIGZldGNoQ29sdW1ucyhkLmRhdGFzZXQuY2F0YWxvZywgZC5kYXRhc2V0LnNjaGVtYSwgZC5kYXRhc2V0LnRhYmxlKS50aGVuKGluZm89PnsKICAgIGJvZHkuaW5uZXJIVE1MID0gaW5mby5jb2x1bW5zLm1hcChjb2w9PgogICAgICAnPGRpdiBjbGFzcz0iY29sLWl0ZW0iPjxkaXYgY2xhc3M9ImNvbCI+PHNwYW4gY2xhc3M9ImNvbC1uYW1lIj4nK2VzYyhjb2wubmFtZSkrJzwvc3Bhbj48c3BhbiBjbGFzcz0iY29sLXR5cGUiPicrZXNjKGR1Y2tkYlR5cGUoY29sLnR5cGUpKSsnPC9zcGFuPjwvZGl2PicrCiAgICAgIChjb2wuY29tbWVudD8gJzxkaXYgY2xhc3M9ImNvbC1jb21tZW50Ij4nK2VzYyhjb2wuY29tbWVudCkrJzwvZGl2Pic6JycpKyc8L2Rpdj4nKS5qb2luKCcnKTsKICB9KTsKfSwgdHJ1ZSk7CgovLyDilIDilIAgZmlsdGVyICsgZXhwYW5kL2NvbGxhcHNlIGFsbCDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKZnVuY3Rpb24gYXBwbHlGaWx0ZXIoKXsKICBjb25zdCBxID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2ZpbHRlcicpLnZhbHVlLnRyaW0oKS50b0xvd2VyQ2FzZSgpOwogIGNvbnN0IHJvb3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnY2F0YWxvZ3MnKTsKICBjb25zdCB0b3RhbCA9IHJvb3QucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlscy50YmwsIGRldGFpbHMuZm4nKS5sZW5ndGg7CiAgbGV0IHNob3duID0gMDsKICByb290LnF1ZXJ5U2VsZWN0b3JBbGwoJ2RldGFpbHMuc2NoZW1hJykuZm9yRWFjaChzYz0+ewogICAgbGV0IGhpdCA9IGZhbHNlOwogICAgc2MucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlscy50YmwsIGRldGFpbHMuZm4nKS5mb3JFYWNoKHJvdz0+ewogICAgICBjb25zdCBtID0gIXEgfHwgcm93LmRhdGFzZXQubmFtZS5pbmNsdWRlcyhxKTsKICAgICAgcm93LmNsYXNzTGlzdC50b2dnbGUoJ2hpZGRlbicsICFtKTsgaWYgKG0peyBzaG93bisrOyBoaXQ9dHJ1ZTsgfQogICAgfSk7CiAgICBzYy5xdWVyeVNlbGVjdG9yQWxsKCcuZ3JwJykuZm9yRWFjaChnPT57CiAgICAgIGNvbnN0IGFueSA9IFsuLi5nLnF1ZXJ5U2VsZWN0b3JBbGwoJ2RldGFpbHMudGJsLCBkZXRhaWxzLmZuJyldLnNvbWUoeD0+IXguY2xhc3NMaXN0LmNvbnRhaW5zKCdoaWRkZW4nKSk7CiAgICAgIGcuY2xhc3NMaXN0LnRvZ2dsZSgnaGlkZGVuJywgcSAmJiAhYW55KTsKICAgIH0pOwogICAgY29uc3QgbmFtZUhpdCA9ICFxIHx8IHNjLmRhdGFzZXQubmFtZS5pbmNsdWRlcyhxKTsKICAgIHNjLmNsYXNzTGlzdC50b2dnbGUoJ2hpZGRlbicsICFoaXQgJiYgIW5hbWVIaXQpOwogICAgaWYgKHEgJiYgKGhpdCB8fCBuYW1lSGl0KSkgc2Mub3BlbiA9IHRydWU7CiAgfSk7CiAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2ZpbHRlci1jb3VudCcpLnRleHRDb250ZW50ID0gcSA/IHNob3duKycgb2YgJyt0b3RhbCA6ICcnOwogIGxldCBuciA9IHJvb3QucXVlcnlTZWxlY3RvcignLm5vcmVzdWx0cycpOwogIGlmIChxICYmIHNob3duPT09MCl7IGlmKCFucil7IG5yPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2RpdicpOyBuci5jbGFzc05hbWU9J25vcmVzdWx0cyc7IG5yLnRleHRDb250ZW50PSdObyBtYXRjaGluZyBvYmplY3RzLic7IHJvb3QuYXBwZW5kQ2hpbGQobnIpO30gfQogIGVsc2UgaWYgKG5yKSBuci5yZW1vdmUoKTsKfQpkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZmlsdGVyJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCBhcHBseUZpbHRlcik7CmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdleHBhbmQtYWxsJykuYWRkRXZlbnRMaXN0ZW5lcignY2xpY2snLCAoKT0+ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2NhdGFsb2dzJykucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlscycpLmZvckVhY2goZD0+ZC5vcGVuPXRydWUpKTsKZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2NvbGxhcHNlLWFsbCcpLmFkZEV2ZW50TGlzdGVuZXIoJ2NsaWNrJywgKCk9PmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjYXRhbG9ncycpLnF1ZXJ5U2VsZWN0b3JBbGwoJ2RldGFpbHMnKS5mb3JFYWNoKGQ9PmQub3Blbj1mYWxzZSkpOwoKLy8g4pSA4pSAIGZvb3RlciArIGZpcnN0IHJlbmRlciDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIDilIAKZnVuY3Rpb24gcmVuZGVyRm9vdGVyKCl7CmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdmaWx0ZXItd3JhcCcpLmluc2VydEFkamFjZW50SFRNTCgnYWZ0ZXJiZWdpbicsIGljb24oJ3NlYXJjaCcpKTsKZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2Zvb3RlcicpLmlubmVySFRNTCA9CiAgJzxhIGhyZWY9IicrcHJlZml4KycvZGVzY3JpYmUiPlJQQyBBUEk8L2E+JysKICAnPGRpdiBjbGFzcz0iZm9vdC1tZXRhIj52Z2ktcnBjICg8Y29kZT4nK2VzYyhkZXNjcmliZS53b3JrZXIubGFuZykrJzwvY29kZT4pIMK3IHYnK2VzYyhkZXNjcmliZS53b3JrZXIudmVyc2lvbikrJyDCtyA8Y29kZT4nK2VzYyhkZXNjcmliZS5zZXJ2ZXJfaWQpKyc8L2NvZGU+PC9kaXY+JysKICAnPGRpdiBjbGFzcz0iZm9vdC1tZXRhIGZvb3QtY29weSI+JmNvcHk7IDIwMjYgXHVEODNEXHVERTlDIFF1ZXJ5IEZhcm0gTExDIC0gPGEgaHJlZj0iaHR0cHM6Ly9xdWVyeS5mYXJtIiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+aHR0cHM6Ly9xdWVyeS5mYXJtPC9hPjwvZGl2Pic7Cn0KCmZ1bmN0aW9uIHJlbmRlckFsbCgpeyByZW5kZXJDYXRCYXIoKTsgcmVuZGVyQ2F0Q2FyZCgpOyByZW5kZXJDb25uZWN0KCk7IHVwZGF0ZUN1cG9sYSgpOyByZW5kZXJDb250ZW50cygpOyB9CmFzeW5jIGZ1bmN0aW9uIGJvb3QoKXsKICB0cnkgewogICAgY29uc3QgcmVzID0gYXdhaXQgZmV0Y2goJ2Rlc2NyaWJlLmpzb24nLCB7aGVhZGVyczp7J0FjY2VwdCc6J2FwcGxpY2F0aW9uL2pzb24nfX0pOwogICAgaWYgKCFyZXMub2spIHRocm93IG5ldyBFcnJvcignSFRUUCAnK3Jlcy5zdGF0dXMpOwogICAgZGVzY3JpYmUgPSBhd2FpdCByZXMuanNvbigpOwogIH0gY2F0Y2ggKGUpIHsKICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdjYXRhbG9ncycpLmlubmVySFRNTCA9CiAgICAgICc8ZGl2IGNsYXNzPSJub3Jlc3VsdHMiPkNvdWxkIG5vdCBsb2FkIHdvcmtlciBtZXRhZGF0YSAoJyArIGUgKyAnKS48L2Rpdj4nOwogICAgcmV0dXJuOwogIH0KICBpZiAoKGRlc2NyaWJlLmxhbmRpbmdfc2NoZW1hX3ZlcnNpb24gfHwgMSkgPiBTVVBQT1JURURfU0NIRU1BKSB7CiAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnY2F0YWxvZ3MnKS5pbm5lckhUTUwgPQogICAgICAnPGRpdiBjbGFzcz0ibm9yZXN1bHRzIj5UaGlzIHdvcmtlciBuZWVkcyBhIG5ld2VyIGxhbmRpbmcgcGFnZSBcdTIwMTQgcGxlYXNlIHJlZnJlc2guPC9kaXY+JzsKICAgIHJldHVybjsKICB9CiAgcmVuZGVySWRlbnRpdHkoKTsgcmVuZGVySGVhZGVyKCk7IHJlbmRlckZvb3RlcigpOyByZW5kZXJBbGwoKTsKfQpib290KCk7Cjwvc2NyaXB0Pgo8L2JvZHk+CjwvaHRtbD4K";
function decodeBase64(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0;i < bin.length; i++)
    out[i] = bin.charCodeAt(i);
  return out;
}
var LANDING_HTML_BYTES = decodeBase64(LANDING_HTML_B64);

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
  const compressionLevel = options?.compressionLevel;
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
  const landingDescribe = options?.landingDescribe ?? null;
  const oauthActive = pkceConfig != null;
  const genericLandingHtml = enableLandingPage && !landingDescribe ? buildLandingPage(displayName, serverId, enableDescribePage ? `${prefix}/describe` : null, repoUrl) : null;
  const describeHtml = enableDescribePage ? buildDescribePage(displayName, serverId, methods, repoUrl) : null;
  const notFoundHtml = enableNotFoundPage ? buildNotFoundPage(prefix, displayName) : null;
  const landingStatusBody = JSON.stringify({ status: "ok", server_id: serverId, protocol: "vgi" });
  const externalLocation = options?.externalLocation;
  const uploadUrlProvider = options?.uploadUrlProvider;
  const maxUploadBytes = options?.maxUploadBytes;
  const stickyEnabled = options?.enableSticky === true;
  const stickyDefaultTtl = options?.stickyDefaultTtl ?? 300;
  const stickyEchoHeadersArr = stickyEnabled ? Object.entries(options?.stickyEchoHeaders ?? {}) : [];
  const sessionRegistry = stickyEnabled ? new SessionRegistry(stickyDefaultTtl) : null;
  const stopReaper = sessionRegistry ? startSessionReaper(sessionRegistry) : null;
  if (options?._onStickyHandle && sessionRegistry) {
    options._onStickyHandle(makeDrainHandle(sessionRegistry, stopReaper ?? undefined));
  }
  const supportedResponseEncodings = [];
  const zstdResponseAvailable = compressionLevel != null && isZstdCompressAvailable();
  if (zstdResponseAvailable) {
    supportedResponseEncodings.push("zstd");
  }
  if (compressionLevel != null) {
    supportedResponseEncodings.push("gzip");
  }
  function addCapabilityHeaders(headers, isOptions = false) {
    if (supportedResponseEncodings.length) {
      headers.set("VGI-Supported-Encodings", supportedResponseEncodings.join(", "));
    }
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
    kind: transportKind
  };
  function addCorsHeaders(headers, isOptions = false, requestedHeaders) {
    if (corsOrigins) {
      headers.set("Access-Control-Allow-Origin", corsOrigins);
      headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
      headers.set("Access-Control-Allow-Headers", requestedHeaders && requestedHeaders.length > 0 ? requestedHeaders : "Content-Type, Authorization");
      headers.set("Access-Control-Expose-Headers", `WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, ${RPC_ERROR_HEADER}, VGI-Max-Response-Bytes, VGI-Max-Externalized-Response-Bytes, VGI-Externalization-Enabled`);
      if (isOptions && corsMaxAge != null) {
        headers.set("Access-Control-Max-Age", String(corsMaxAge));
      }
    }
  }
  async function compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip) {
    if (compressionLevel == null)
      return response;
    const codec = clientAcceptsZstd && zstdResponseAvailable ? "zstd" : clientAcceptsGzip ? "gzip" : null;
    if (!codec)
      return response;
    const responseBody = new Uint8Array(await response.arrayBuffer());
    const compressed = codec === "zstd" ? await zstdCompress(responseBody, compressionLevel) : await gzipCompress(responseBody);
    const headers = new Headers(response.headers);
    headers.set("Content-Encoding", codec);
    return new Response(compressed, {
      status: response.status,
      headers
    });
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
  return async function handler(request) {
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
      if (corsOrigins || maxRequestBytes != null || maxResponseBytes != null || maxExternalizedResponseBytes != null || uploadUrlProvider || stickyEnabled || path === `${prefix}/__capabilities__`) {
        return new Response(null, { status: 204, headers });
      }
      return new Response(null, { status: 405 });
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
      if (landingDescribe) {
        if (path === prefix || path === `${prefix}/`) {
          const accept = request.headers.get("Accept") ?? "";
          const wantJson = url.searchParams.get("format") === "json" || accept.includes("application/json") && !accept.includes("text/html");
          if (wantJson) {
            const headers2 = new Headers({ "Content-Type": "application/json" });
            addCorsHeaders(headers2);
            return new Response(landingStatusBody, { status: 200, headers: headers2 });
          }
          const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
          addCorsHeaders(headers);
          return new Response(LANDING_HTML_BYTES, { status: 200, headers });
        }
        if (path === `${prefix}/describe.json`) {
          const doc = await landingDescribe.describe({ serverId, oauth: oauthActive });
          const headers = new Headers({ "Content-Type": "application/json" });
          addCorsHeaders(headers);
          return new Response(JSON.stringify(doc), { status: 200, headers });
        }
        if (path.startsWith(`${prefix}/describe/`) && path.endsWith(".json")) {
          const rest = path.slice(`${prefix}/describe/`.length, -".json".length);
          const parts = rest.split("/");
          if (parts.length === 3) {
            const [cat, sch, tbl] = parts.map((p) => decodeURIComponent(p));
            const cols = await landingDescribe.columns(cat, sch, tbl);
            const headers = new Headers({ "Content-Type": "application/json" });
            addCorsHeaders(headers);
            if (cols == null) {
              return new Response(JSON.stringify({ error: "object not found" }), { status: 404, headers });
            }
            return new Response(JSON.stringify(cols), { status: 200, headers });
          }
        }
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
    const ctx = { ...baseCtx, cookies: parseRequestCookies(request) };
    if (authenticate) {
      try {
        ctx.authContext = await authenticate(request);
      } catch (error) {
        const headers = new Headers({ "Content-Type": "text/plain" });
        addCorsHeaders(headers);
        if (oauthMetadata) {
          const metadataUrl = new URL(request.url);
          metadataUrl.pathname = wellKnownPath(prefix);
          metadataUrl.search = "";
          headers.set("WWW-Authenticate", buildWwwAuthenticateHeader(metadataUrl.toString(), oauthMetadata.clientId, oauthMetadata.clientSecret, oauthMetadata.useIdTokenAsBearer, oauthMetadata.deviceCodeClientId, oauthMetadata.deviceCodeClientSecret));
        }
        return new Response(error.message || "Unauthorized", { status: 401, headers });
      }
    }
    const acceptEncodingEarly = (request.headers.get("Accept-Encoding") ?? "").toLowerCase();
    const clientAcceptsZstdEarly = acceptEncodingEarly.includes("zstd");
    const clientAcceptsGzipEarly = acceptEncodingEarly.includes("gzip");
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
          return compressIfAccepted(r, clientAcceptsZstdEarly, clientAcceptsGzipEarly);
        }
        const entry = sessionRegistry.get(opened.sessionId, principalKey);
        if (!entry) {
          const r = makeErrorResponse(new SessionLostError("session not found, expired, or principal mismatch"), 500);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, clientAcceptsZstdEarly, clientAcceptsGzipEarly);
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
    const clientAcceptsZstd = clientAcceptsZstdEarly;
    const clientAcceptsGzip = clientAcceptsGzipEarly;
    let body = new Uint8Array(await request.arrayBuffer());
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
        return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
      } catch (error) {
        if (error instanceof HttpRpcError) {
          const r2 = makeErrorResponse(error, error.statusCode, UPLOAD_URL_RESPONSE_SCHEMA);
          addCapabilityHeaders(r2.headers);
          return compressIfAccepted(r2, clientAcceptsZstd, clientAcceptsGzip);
        }
        const r = makeErrorResponse(error, 500, UPLOAD_URL_RESPONSE_SCHEMA);
        addCapabilityHeaders(r.headers);
        return compressIfAccepted(r, clientAcceptsZstd, clientAcceptsGzip);
      }
    }
    if (path === `${prefix}/${DESCRIBE_METHOD_NAME}`) {
      try {
        const response = await httpDispatchDescribe(protocol.name, methods, serverId, protocol.protocolVersion || undefined);
        addCorsHeaders(response.headers);
        return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
      } catch (error) {
        return compressIfAccepted(makeErrorResponse(error, 500), clientAcceptsZstd, clientAcceptsGzip);
      }
    }
    if (!path.startsWith(`${prefix}/`)) {
      return new Response("Not Found", { status: 404 });
    }
    const subPath = path.slice(prefix.length + 1);
    let methodName;
    let action;
    if (subPath.endsWith("/init")) {
      methodName = subPath.slice(0, -5);
      action = "init";
    } else if (subPath.endsWith("/exchange")) {
      methodName = subPath.slice(0, -9);
      action = "exchange";
    } else {
      methodName = subPath;
      action = "call";
    }
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err2 = new MethodNotImplementedError(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      return compressIfAccepted(makeErrorResponse(err2, 404), clientAcceptsZstd, clientAcceptsGzip);
    }
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
        return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
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
      requestId: null,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: transportKind,
      principal: auth?.principal ?? "",
      authDomain: auth?.domain ?? "",
      authenticated: auth?.authenticated ?? false,
      requestData: action === "call" ? body : undefined
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
      return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
    } catch (error) {
      dispatchError = error instanceof Error ? error : new Error(String(error));
      if (error instanceof HttpRpcError) {
        const r2 = makeErrorResponse(error, error.statusCode);
        addCapabilityHeaders(r2.headers);
        applyStickyResponseHeaders(r2.headers, stickySink);
        return compressIfAccepted(r2, clientAcceptsZstd, clientAcceptsGzip);
      }
      const r = makeErrorResponse(error, 500);
      addCapabilityHeaders(r.headers);
      applyStickyResponseHeaders(r.headers, stickySink);
      return compressIfAccepted(r, clientAcceptsZstd, clientAcceptsGzip);
    } finally {
      if (stickySink) {
        if (stickySink.sessionId)
          info.sessionId = stickySink.sessionId;
        info.sessionAction = stickySink.action;
      }
      dispatchHook?.onDispatchEnd(hookToken, info, stats, dispatchError);
      if (stickyLockRelease) {
        try {
          stickyLockRelease();
        } catch {}
        stickyLockRelease = null;
      }
    }
  };
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

// src/wire/writer.ts
var STDOUT_FD = 1;
var _NODE_FS_MOD2 = "node:fs";
var _writeSync = null;
function _loadWriteSync2() {
  if (_writeSync)
    return _writeSync;
  const req = import.meta.require ?? globalThis.require ?? null;
  if (!req) {
    throw new Error("IpcStreamWriter requires Bun or Node.js CJS for sync node:fs.writeSync. " + "Subprocess transport is not available in this runtime.");
  }
  const fs = req(_NODE_FS_MOD2);
  _writeSync = fs.writeSync.bind(fs);
  return _writeSync;
}
function writeAll(fd, data) {
  const writeSync = _loadWriteSync2();
  let offset = 0;
  while (offset < data.length) {
    try {
      const written = writeSync(fd, data, offset, data.length - offset);
      if (written <= 0)
        throw new Error(`writeSync returned ${written}`);
      offset += written;
    } catch (e) {
      if (e.code === "EAGAIN") {
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
        continue;
      }
      throw e;
    }
  }
}
async function socketWriteAll(socket, data) {
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
    const bytes2 = serializeBatches(schema2, batches);
    if (this.target.kind === "fd") {
      writeAll(this.target.fd, bytes2);
    } else if (this.target.kind === "sink") {
      await this.target.sink.write(bytes2);
    } else {
      await socketWriteAll(this.target.socket, bytes2);
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
  enqueue(bytes2) {
    const next = this.writeChain.then(() => {
      if (this.target.kind === "fd") {
        writeAll(this.target.fd, bytes2);
        return;
      }
      if (this.target.kind === "sink") {
        return this.target.sink.write(bytes2);
      }
      return socketWriteAll(this.target.socket, bytes2);
    });
    this.writeChain = next.catch(() => {
      return;
    });
    return next;
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
    const methods = this.protocol.getMethods();
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
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
    try {
      requestData = serializeBatch(batch);
    } catch {}
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
  return serializeIpcStream(schema2, [buildErrorBatch(schema2, error, serverId, requestId)]);
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
export {
  writeUnaryResult,
  writeRequest,
  unpackStateToken,
  uint82 as uint8,
  uint642 as uint64,
  uint322 as uint32,
  uint162 as uint16,
  tryAcquireLock,
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
  oauthResourceMetadataToJson,
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
  HttpStreamSession,
  FdSink,
  ERROR_KIND_SESSION_LOST,
  ERROR_KIND_SERVER_DRAINING,
  ERROR_KIND_METHOD_NOT_IMPLEMENTED,
  ERROR_KIND_KEY,
  DESCRIBE_VERSION_KEY,
  DESCRIBE_VERSION,
  DESCRIBE_METHOD_NAME,
  AuthContext,
  AccessLogHook,
  ARROW_CONTENT_TYPE
};

//# debugId=85531258E103CA9964756E2164756E21
