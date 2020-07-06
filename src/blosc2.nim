import strformat
import os

{.passL: "-lblosc2 -lpthread".}
{.pragma: blosc2, importc, header: "<blosc2.h>".}

const BLOSC_MAX_OVERHEAD = 32

proc blosc_get_blocksize(): cint {.blosc2.}
proc blosc_set_blocksize(): cint {.blosc2.}
proc blosc_list_compressors*(): cstring {.blosc2.}
proc blosc_init*() {.blosc2.}
proc blosc_destroy() {.blosc2.}
proc blosc_set_delta(dodelta:cint) {.discardable, blosc2.}
proc blosc_set_nthreads(nthreads:cint): cint {.discardable, blosc2.}
proc blosc_set_compressor(compname:cstring): cint {.blosc2.}

proc blosc_compname_to_compcode(compname:cstring): cint {.blosc2.}

proc blosc_compress(clevel:cint, doshuffle:cint, typesize: csize_t,
    nbytes:csize_t, src: pointer, dest:pointer, destsize:csize_t): cint {.blosc2.}

proc blosc_decompress(src:pointer, dest:pointer, destsize:csize_t): cint {.blosc2.}

type BloscLib* {.pure.} = enum
  BLOSCLZ = 0
  LZ4 = 1
  LZ4HC = 2
  SNAPPY = 3
  ZLIB = 4
  ZSTD = 5
  LIZARD = 6
  MAX_CODECS = 7         ## !< maximum number of reserved codecs

type BloscFilters* {.pure, size: 1.} = enum
  #NOSHUFFLE = 0          ## !< no shuffle (for compatibility with Blosc1)
  NOFILTER = 0'u8           ## !< no filter
  SHUFFLE = 1            ## !< byte-wise shuffle
  BITSHUFFLE = 2         ## !< bit-wise shuffle
  DELTA = 3              ## !< delta filter
  TRUNC_PREC = 4         ## !< truncate precision filter
  LAST_FILTER = 5        ## !< sentinel


const BLOSC2_MAX_FILTERS = 6

converter toCsize*(i:int): csize_t =
  i.csize_t
converter toCint*(i:int): cint =
  i.cint

proc compress*[T](input:var seq[T], shuffle:bool=false, delta:bool=false, nthreads:int=2, clevel:int=5): seq[uint8] =
  blosc_set_delta(delta.cint)
  blosc_set_nthreads(nthreads)
  result = newSeqUninitialized[uint8](5 * input.len)
  let n = blosc_compress(clevel, shuffle.cint, sizeof(input[0]), sizeof(input[0]) * input.len, input[0].addr.pointer,
                result[0].addr.pointer, result[0].sizeof * result.len)
  doAssert n > 0, "error compressing input"
  result.setLen(int(n / sizeof(result[0])))

proc decompress*[T:Ordinal|float32|float64](compressed: var seq[uint8], out_len:int=0): seq[T] =
  var out_len = out_len
  if out_len == 0: out_len = len(compressed)
  result = newSeqUninitialized[T](out_len)
  let n = blosc_decompress(compressed[0].addr.pointer, result[0].addr.pointer, out_len * sizeof(T))
  doAssert n > 0, "error decompressing"
  result.setLen(int(n / sizeof(T)))

type blosc2_context* = pointer #{.blosc2, incompleteStruct.} = object

type Context* = blosc2_context

type blosc2_prefilter_params* {.blosc2.} = object
    user_data*: pointer        ##  user-provided info (optional)
    `out`*: ptr uint8         ##  the output buffer
    out_size*: int32         ##  the output size (in bytes)
    out_typesize*: int32     ##  the output typesize
    out_offset*: int32       ##  offset to reach the start of the output buffer
    tid*: int32              ##  thread id
    ttmp*: ptr uint8          ##  a temporary that is able to hold several blocks for the output and is private for each thread
    ttmp_nbytes*: int32      ##  the size of the temporary in bytes
    ctx*: pointer    ##  the decompression context

type blosc2_prefilter_fn* = proc (params: ptr blosc2_prefilter_params): cint {.stdcall.}

type blosc2_cparams* {.blosc2.} = object
    compcode*: uint8         ## !< The compressor codec.
    clevel*: uint8           ## !< The compression level (5).
    use_dict*: cint            ## !< Use dicts or not when compressing (only for ZSTD).
    typesize*: int32         ## !< The type size (8).
    nthreads*: int16         ## !< The number of threads to use internally (1).
    blocksize*: int32        ## !< The requested size of the compressed blocks (0; meaning automatic).
    schunk*: pointer           ## !< The associated schunk, if any (NULL).
    filters*: array[BLOSC2_MAX_FILTERS, uint8] ## !< The (sequence of) filters.
    filters_meta*: array[BLOSC2_MAX_FILTERS, uint8] ## !< The metadata for filters.
    prefilter*: blosc2_prefilter_fn ## !< The prefilter function.
    pparams*: ptr blosc2_prefilter_params ## !< The prefilter parameters.


var BLOSC2_CPARAMS_DEFAULTS*: blosc2_cparams = blosc2_cparams(
    compcode:BloscLib.BLOSCLZ.uint8,
    clevel:5, use_dict:0, typesize:8,
    nthreads:1, block_size:0,
    schunk:nil,
    filters: [0'u8, 0, 0, 0, 0, BloscFilters.SHUFFLE.uint8],
    filters_meta: [0'u8, 0, 0, 0, 0, 0],
    prefilter:nil,
    pparams:nil)

type blosc2_dparams* {.blosc2.} = object
    nthreads*: cint            ## !< The number of threads to use internally (1).
    schunk*: pointer           ## !< The associated schunk, if any (NULL).

const
  BLOSC2_MAX_METALAYERS* = 16
  BLOSC2_METALAYER_NAME_MAXLEN* = 31

type
  blosc2_frame* {.bycopy.} = object
    fname*: cstring            ## !< The name of the file; if NULL, this is in-memory
    sdata*: ptr uint8         ## !< The in-memory serialized data
    coffsets*: ptr uint8      ## !< Pointers to the (compressed, on-disk) chunk offsets
    len*: int64              ## !< The current length of the frame in (compressed) bytes
    maxlen*: int64           ## !< The maximum length of the frame; if 0, there is no maximum
    trailer_len*: uint32     ## !< The current length of the trailer in (compressed) bytes


## *
##  @brief This struct is meant to store metadata information inside
##  a #blosc2_schunk, allowing to specify, for example, how to interpret
##  the contents included in the schunk.
##
type blosc2_metalayer* {.bycopy.} = object
    name*: cstring             ## !< The metalayer identifier for Blosc client (e.g. Caterva).
    content*: ptr uint8       ## !< The serialized (msgpack preferably) content of the metalayer.
    content_len*: int32      ## !< The length in bytes of the content.

type blosc2_schunk* {.bycopy.} = object
    version*: uint8
    compcode*: uint8         ## !< The default compressor. Each chunk can override this.
    clevel*: uint8           ## !< The compression level and other compress params.
    typesize*: int32         ## !< The type size.
    blocksize*: int32        ## !< The requested size of the compressed blocks (0; meaning automatic).
    chunksize*: int32        ## !< Size of each chunk. 0 if not a fixed chunksize.
    filters*: array[BLOSC2_MAX_FILTERS, uint8] ## !< The (sequence of) filters.  8-bit per filter.
    filters_meta*: array[BLOSC2_MAX_FILTERS, uint8] ## !< Metadata for filters. 8-bit per meta-slot.
    nchunks*: int32          ## !< Number of chunks in super-chunk.
    nbytes*: int64           ## !< The data size + metadata size + header size (uncompressed).
    cbytes*: int64           ## !< The data size + metadata size + header size (compressed).
    data*: ptr ptr uint8       ## !< Pointer to chunk data pointers buffer.
    data_len*: csize           ## !< Length of the chunk data pointers buffer.
    frame*: ptr blosc2_frame    ## !< Pointer to frame used as store for chunks.
                          ## !<uint8* ctx;
                          ## !< Context for the thread holder. NULL if not acquired.
    cctx*: ptr blosc2_context   ## !< Context for compression
    dctx*: ptr blosc2_context   ## !< Context for decompression.
    metalayers*: array[BLOSC2_MAX_METALAYERS, ptr blosc2_metalayer] ## !< The array of metalayers.
    nmetalayers*: int16      ## !< The number of metalayers in the frame
    usermeta*: ptr uint8      ## <! The user-defined metadata.
    usermeta_len*: int32     ## <! The (compressed) length of the user-defined metadata.

# schunk stuff
proc blosc2_new_schunk*(cparams: blosc2_cparams; dparams: blosc2_dparams;
                       frame: ptr blosc2_frame): ptr blosc2_schunk {.blosc2.}
proc blosc2_free_schunk*(schunk: ptr blosc2_schunk): cint {.blosc2.}
proc blosc2_schunk_append_chunk*(schunk: ptr blosc2_schunk; chunk: ptr uint8;
                                copy: bool): cint {.blosc2.}
proc blosc2_schunk_append_buffer*(schunk: ptr blosc2_schunk; src: pointer;
                                 nbytes: csize_t): cint {.blosc2.}
proc blosc2_schunk_decompress_chunk*(schunk: ptr blosc2_schunk; nchunk: cint;
                                    dest: pointer; nbytes: csize_t): cint {.blosc2.}
proc blosc2_schunk_get_chunk*(schunk: ptr blosc2_schunk; nchunk: cint;
                             chunk: ptr ptr uint8; needs_free: ptr bool): cint {.blosc2.}

# frames:

proc blosc2_new_frame*(fname:cstring): ptr blosc2_frame {.blosc2.}
proc blosc2_schunk_to_frame(schunk: ptr blosc2_schunk, frame: ptr blosc2_frame): int64 {.blosc2.}
proc blosc2_free_frame*(frame:ptr blosc2_frame): cint {.blosc2.}
proc blosc2_frame_to_file*(frame: ptr blosc2_frame, fname: cstring): int64 {.blosc2.};
proc blosc2_frame_from_file(fname:cstring): ptr blosc2_frame {.blosc2.}
proc blosc2_schunk_from_frame*(frame: ptr blosc2_frame, copy:bool): ptr blosc2_schunk {.blosc2.}


## *
##  @brief Default struct for decompression params meant for user initialization.
##
proc blosc2_create_cctx(params: blosc2_cparams): blosc2_context {.blosc2.}
proc blosc2_create_dctx(params: blosc2_dparams): blosc2_context {.blosc2.}
proc blosc2_free_ctx(ctx: blosc2_context) {.blosc2.}

proc blosc2_compress_ctx(ctx:blosc2_context, nbytes: csize_t, src: pointer, dest: pointer, destsize:csize_t): int {.blosc2.}


proc blosc2_decompress_ctx(ctx:blosc2_context, src:pointer, dest:pointer, destsize:csize_t): cint {.blosc2.}

var BLOSC2_DPARAMS_DEFAULTS = blosc2_dparams(nthreads:1.cint, schunk:nil)

type Action* {.pure.} = enum
  Compress
  Decompress

proc blosc2_getitem_ctx(context:blosc2_context, src:pointer, start:cint, nitems:cint, dest:pointer): cint {.blosc2.}

proc getitem*[T](ctx:blosc2_context, src:var seq[uint8], start:int, output:var seq[T]) =
  let bytes = ctx.blosc2_getitem_ctx(src[0].addr.pointer, start.cint, output.len.cint, output[0].addr.pointer)
  if bytes != output.len * sizeof(T):
    raise newException(IOError, "blosc2: error in getitem, unexpected number of bytes")

proc blosc_cbuffer_sizes(cbuffer:pointer, nbytes:ptr csize_t, cbytes:ptr csize_t, blocksize:ptr csize_t) {.blosc2.}
proc blosc_cbuffer_metainfo(cbuffer:pointer, typesize: ptr csize_t, flags: ptr cint) {.blosc2.}
proc blosc_cbuffer_complib(cbuffer:pointer): cstring {.blosc2.}

proc buffer_info*(buffer: var seq[uint8]): tuple[uncompressed_bytes: int, compressed_bytes:int, blocksize:int, typesize:int, flags: int, complib:string] =
  ## given a compressed buffer report the compressed, uncompressed, and block-size
  var ub:csize_t
  var cb:csize_t
  var bs: csize_t
  buffer[0].addr.pointer.blosc_cbuffer_sizes(ub.addr, cb.addr, bs.addr)
  result.uncompressed_bytes = ub.int
  result.compressed_bytes = cb.int
  result.block_size = bs.int
  var ts:csize_t
  var flags:cint
  buffer[0].addr.pointer.blosc_cbuffer_metainfo(ts.addr, flags.addr)
  result.typesize = ts.int
  result.flags = flags.int
  result.complib = $buffer[0].addr.pointer.blosc_cbuffer_complib

proc create_cparams[T](codec:string, clevel:int=5, delta:bool=false, threads:int=4, use_dict:bool=false, schunk:pointer=nil): blosc2_cparams =
  var ctx = blosc2_cparams()
  ctx.compcode = blosc_compname_to_compcode(codec).uint8
  ctx.clevel = clevel.uint8
  ctx.typesize = sizeof(T)
  ctx.schunk = schunk
  ctx.nthreads = threads.int16
  ctx.filters = [0'u8, 0, 0, 0, 0, BloscFilters.SHUFFLE.uint8]
  if delta:
    ctx.filters[0] = BloscFilters.DELTA.uint8
  ctx.filters_meta =  [0'u8, 0, 0, 0, 0, 0]
  ctx.prefilter = nil
  ctx.pparams = nil
  return ctx

proc compressContext*[T](codec:string, clevel:int=5, delta:bool=false, threads:int=4, use_dict:bool=false, schunk:pointer=nil): blosc2_context =
  var ctx = create_cparams[T](codec, clevel, delta, threads, use_dict, schunk)
  return blosc2_create_cctx(ctx)

proc decompressContext*(threads:int|int16=4, schunk:pointer=nil): blosc2_context =
  var ctx = blosc2_dparams()
  ctx.nthreads = threads.int16
  ctx.schunk = schunk
  return blosc2_create_dctx(ctx)

proc freeContext*(ctx:blosc2_context) =
  blosc2_free_ctx(ctx)

type Frame = ref object
  c: ptr blosc2_frame

type superChunk*[T] = ref object
  c*: ptr blosc2_schunk
  frame*: Frame

proc destroy_frame(f:Frame) =
  if f.c != nil:
    discard f.c.blosc2_free_frame

proc destroy_chunk(c:superChunk) =
  if c.c != nil:
    discard c.c.blosc2_free_schunk

proc newFrame*(path:string, mode:FileMode=fmWrite): Frame =
  new(result, destroy_frame)
  if path == "":
      result.c = blosc2_new_frame(nil)
      return

  if mode in {fmRead}:
    doAssert path != "", "expected path with writable mode"
    result.c = blosc2_frame_from_file(path)
  elif mode == fmWrite:
    result.c = blosc2_new_frame(path)
  if result.c == nil:
    raise newException(IOError, "blosc2: error opening file:" & path)

proc newSuperChunk*[T](codec:string="blosclz", clevel:int=5, delta:bool=false, threads:int=4, frame:Frame=nil, newChunk:bool=true): superChunk[T] =
  new(result, destroy_chunk)
  if frame != nil and not newChunk:
    result.c = blosc2_schunk_from_frame(frame.c, false)
    result.frame = frame
    return

  var cparams = create_cparams[T](codec, clevel, delta, threads, false, nil)
  var dparams = blosc2_dparams(nthreads:threads.int16, schunk:nil)
  if frame != nil:
    result.frame = frame
    result.c = blosc2_new_schunk(cparams, dparams, result.frame.c)
  else:
    result.c = blosc2_new_schunk(cparams, dparams, nil)

proc len*[T](s:superChunk[T]): int {.inline.} =
  ## number of chunks in the super chunk
  result = s.c.n_chunks

proc add*[T](s:superChunk[T], input: var seq[T], newChunk:bool=false): int {.discardable.} =
  result = s.c.blosc2_schunk_append_buffer(input[0].addr.pointer, sizeof(T) * input.len)

proc into*[T](s:superChunk[T], i:int, output: var seq[T]) =
  if i < 0 or i > s.len: raise newException(IndexError, &"chunk {i} is out of bounds in superchunk with len: {s.len}")

  var chunk:ptr uint8
  var needs_free:bool
  let res = s.c.blosc2_schunk_get_chunk(i.cint, chunk.addr, needs_free.addr)
  if res < 0:
    raise newException(IndexError, "error:" & $res & " accessing chunk:" & $i)
  proc free(a1: pointer) {.cdecl, importc: "free", header: "<stdlib.h>".}

  var ub:csize_t
  var cb:csize_t
  var bs:csize_t
  chunk.pointer.blosc_cbuffer_sizes(ub.addr, cb.addr, bs.addr)
  if needs_free:
    free(chunk)

  output.setLen(int(ub.int / sizeof(T)))
  doAssert s.c.blosc2_schunk_decompress_chunk(i.cint, output[0].addr.pointer, ub) == ub.cint, "into: unexpected size of decompressed chunk"


proc compress*[T](ctx:blosc2_context, input:var seq[T], output: var seq[uint8], adjustOutputSize:bool=false) =
  if adjustOutputSize:
    output.setLen(input[0].sizeof * input.len + BLOSC_MAX_OVERHEAD)
  let size = ctx.blosc2_compress_ctx(input[0].sizeof * input.len, input[0].addr.pointer, output[0].addr.pointer, output[0].sizeof * output.len)
  if size <= 0:
    raise newException(IOError, "blosc2: error decompressing")
  output.setLen(size)

proc compress*[T](ctx:blosc2_context, input:var seq[T]): seq[uint8] =
  ctx.compress(input, result, true)

proc decompress*[T](ctx:blosc2_context, compressed:var seq[uint8], output: var seq[T]) =
  var bi = compressed.buffer_info
  output.setLen(int(bi.uncompressed_bytes / sizeof(T)))
  let size = ctx.blosc2_decompress_ctx(compressed[0].addr.pointer, output[0].addr.pointer, output[0].sizeof * output.len)
  if size != bi.uncompressed_bytes:
    raise newException(IOError, "blosc2: error decompressing")
