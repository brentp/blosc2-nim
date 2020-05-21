
{.passL: "-lblosc2 -lpthread".}
{.pragma: blosc2, importc, header: "<blosc2.h>".}

const BLOSC_MAX_OVERHEAD = 32

proc blosc_get_blocksize(): cint {.blosc2.}
proc blosc_set_blocksize(): cint {.blosc2.}
proc blosc_list_compressors*(): cstring {.blosc2.}
proc blosc_init() {.blosc2.}
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

proc compressContext*[T](codec:string, clevel:int=5, delta:bool=false, threads:int=4, use_dict:bool=false, schunk:pointer=nil): blosc2_context =
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
  return blosc2_create_cctx(ctx)

proc decompressContext*(threads:int|int16=4, schunk:pointer=nil): blosc2_context =
  var ctx = blosc2_dparams()
  ctx.nthreads = threads.int16
  ctx.schunk = schunk
  return blosc2_create_dctx(ctx)

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
  let size = ctx.blosc2_decompress_ctx(compressed[0].addr.pointer, output[0].addr.pointer, output[0].sizeof * output.len)
  if size <= 0:
    raise newException(IOError, "blosc2: error decompressing")
  output.setLen(int(size / sizeof(T)))

when isMainModule:
  blosc_init()
  echo blosc_list_compressors()
  echo blosc_set_compressor("blosclz")
  blosc_set_delta(1)
  blosc_set_nthreads(4)

  var x = newSeq[int64](1024)
  for i in 0..x.high:
    x[i] = 8'i32 * i.int64

  #var compressed = newSeq[uint8](400)
  #echo "OK"

  #echo "compressed:", blosc_compress(9.cint, 1.cint, x[0].sizeof.csize_t, x[0].sizeof.csize_t * x.len.csize_t, x[0].addr.pointer, compressed[0].addr.pointer, compressed[0].sizeof * compressed.len)
  #echo x[0].sizeof * x.len
  var compressed = x.compress(delta=true, shuffle=true)
  echo compressed.len

  var decomp = decompress[int64](compressed, len(x))

  echo decomp[0..10]

  block:

    #var cp = BLOSC2_CPARAMS_DEFAULTS
    #cp.typesize = 4
    var ctx = compressContext[int32]("blosclz", delta=false)
    var dctx = decompressContext()
    var x = newSeq[int32](400)
    for i in 0..x.high:
      x[i] = 8'i32 * i.int32

    var compressed = ctx.compress(x)
    echo "compressed bytes:", compressed.len
    echo "uncompressed bytes:", x.len * sizeof(x[0])
    #echo "OK"
    #echo x[0].sizeof

    #echo "cctx:", ctx.blosc2_compress_ctx(x[0].sizeof * x.len, x[0].addr.pointer, compressed[0].addr.pointer, compressed[0].sizeof * compressed.len)

    for i in x.mitems: i = 0
    doAssert x[10] == 0
    dctx.decompress(compressed, x)
    echo x[0..10]



  blosc_destroy()
