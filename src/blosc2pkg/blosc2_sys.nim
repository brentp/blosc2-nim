import strformat
import os

{.passL: "-lblosc2 -lpthread".}
{.pragma: blosc2, importc, header: "<blosc2.h>".}

const BLOSC_MAX_OVERHEAD* = 32

proc blosc_get_blocksize*(): cint {.blosc2.}
proc blosc_set_blocksize*(): cint {.blosc2.}
proc blosc_list_compressors*(): cstring {.blosc2.}
proc blosc_init*() {.blosc2.}
proc blosc_destroy*() {.blosc2.}
proc blosc_set_delta*(dodelta:cint) {.discardable, blosc2.}
proc blosc_set_nthreads*(nthreads:cint): cint {.discardable, blosc2.}
proc blosc_set_compressor*(compname:cstring): cint {.blosc2.}

proc blosc_compname_to_compcode*(compname:cstring): cint {.blosc2.}

proc blosc_compress*(clevel:cint, doshuffle:cint, typesize: csize_t,
    nbytes:csize_t, src: pointer, dest:pointer, destsize:csize_t): cint {.blosc2.}

proc blosc_decompress*(src:pointer, dest:pointer, destsize:csize_t): cint {.blosc2.}

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
proc blosc2_schunk_to_frame*(schunk: ptr blosc2_schunk, frame: ptr blosc2_frame): int64 {.blosc2.}
proc blosc2_free_frame*(frame:ptr blosc2_frame): cint {.blosc2.}
proc blosc2_frame_to_file*(frame: ptr blosc2_frame, fname: cstring): int64 {.blosc2.};
proc blosc2_frame_from_file*(fname:cstring): ptr blosc2_frame {.blosc2.}
proc blosc2_schunk_from_frame*(frame: ptr blosc2_frame, copy:bool): ptr blosc2_schunk {.blosc2.}

# metalayers
proc blosc2_has_metalayer*(shunk: ptr blosc2_schunk, name:cstring): cint {.blosc2.}
# return index if successful, else negative
proc blosc2_add_metalayer*(shunk: ptr blosc2_schunk, name:cstring, content: ptr uint8, length:uint32): int {.blosc2.}
proc blosc2_get_metalayer*(shunk: ptr blosc2_schunk, name:cstring, content: ptr ptr uint8, length:ptr uint32): int {.blosc2.}


## *
##  @brief Default struct for decompression params meant for user initialization.
##
proc blosc2_create_cctx*(params: blosc2_cparams): blosc2_context {.blosc2.}
proc blosc2_create_dctx*(params: blosc2_dparams): blosc2_context {.blosc2.}
proc blosc2_free_ctx*(ctx: blosc2_context) {.blosc2.}

proc blosc2_compress_ctx*(ctx:blosc2_context, nbytes: csize_t, src: pointer, dest: pointer, destsize:csize_t): int {.blosc2.}


proc blosc2_decompress_ctx*(ctx:blosc2_context, src:pointer, dest:pointer, destsize:csize_t): cint {.blosc2.}

var BLOSC2_DPARAMS_DEFAULTS* = blosc2_dparams(nthreads:1.cint, schunk:nil)

proc blosc2_getitem_ctx*(context:blosc2_context, src:pointer, start:cint, nitems:cint, dest:pointer): cint {.blosc2.}

proc getitem*[T](ctx:blosc2_context, src:var seq[uint8], start:int, output:var seq[T]) =
  let bytes = ctx.blosc2_getitem_ctx(src[0].addr.pointer, start.cint, output.len.cint, output[0].addr.pointer)
  if bytes != output.len * sizeof(T):
    raise newException(IOError, "blosc2: error in getitem, unexpected number of bytes")

proc blosc_cbuffer_sizes*(cbuffer:pointer, nbytes:ptr csize_t, cbytes:ptr csize_t, blocksize:ptr csize_t) {.blosc2.}
proc blosc_cbuffer_metainfo*(cbuffer:pointer, typesize: ptr csize_t, flags: ptr cint) {.blosc2.}
proc blosc_cbuffer_complib*(cbuffer:pointer): cstring {.blosc2.}
