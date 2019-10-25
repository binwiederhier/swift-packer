# swift-packer

`packer` is a *proof-of-concept* implementation of an inline packer for OpenStack Swift to overcome the *lots of small files* (LOSF) problem. `packer` acts as a proxy and sits between the application and Swift. It buffers small PUT requests and packs them into a single Swift object before forwarding. This greatly reduces the number of objects in Swift and can dramatically increase PUT throughput, because it reduces the number of objects Swift has to manage.

Each PUT returns a header `X-Item-Path` that serves as the logical path of the small file. If an item is packed by `packer`, the original path is ignored. Using `X-Item-Path`, objects within a pack can be retrieved via GET and deleted via DELETE. Under the hood, `packer` simply translates GET requests against a packed item into a `Range:` request. A DELETE will mark items in a pack as deleted until a repack threshold is reached. Once this threshold is reached, the item is re-packed. This mechanism is a trade-off between DELETE speed and space efficiency, and it also combats file system fragmentation.
 
## Usage
Build the packer binary by running `go build cmd/packer/packer.cmd`. Then simple run `packer my-swift.lan:8080`.

`packer` provides the following options:

```
Syntax: packer [OPTIONS] HOST:PORT
Options:
  -debug
    	Enable debug mode
  -listen string
    	Listen address for packer service (default ":1234")
  -maxcount int
    	Max items per pack (default 10)
  -maxwait int
    	Wait time in milliseconds for downstream PUTs before closing pack (default 100)
  -minsize string
    	Minimum pack size (default "128k")
  -repack int
    	Repack after X% of a pack have been deleted (default 20)
```

## Swift configuration
`packer` uses object metadata to track a pack's contents. Depending on how many items are packed into one Swift object, it may be necessary to increase the maximum header and metadata value size. 

Add this to your `swift.conf` file:

```
[swift-constraints]
max_header_size=16384
max_meta_value_length=8192
```

## Example

Start packer in one terminal window in debug mode:
```
$ ./packer -debug my-swift.lan:8080
```

Then PUT a couple of small files towards the proxy, using the `X-Allow-Pack: yes` header and an optional `X-Pack-Group:` header:

```
for p in AAAA BBBBBBBB CC DD EEEEEEEEEEEEEEE; do
  curl -v \
    -XPUT \
    -d $p \
    -H "X-Allow-Pack: yes" \
    -H "X-Pack-Group: group1" \
    -H "X-Auth-Token: ...." \
     localhost:1234/v1/AUTH_phil/cont1/$p &
done
```

You will see a bunch of `curl` output. Pay attention to the `X-Item-Path` headers:

```
HTTP/1.1 201 Created
X-Item-Path: AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729/2
X-Pack-Path: AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729
...
```

You can get individual items via GET:

```
$ curl \ 
  -H 'X-Auth-Token: ...' \
  localhost:1234/v1/AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729/2
CC
```

Or delete them via DELETE:

```
$ curl \
  -X DELETE \
  -H 'X-Auth-Token: ...' \
  localhost:1234/AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729/2

$ curl \
  -H 'X-Auth-Token: ...' \
  localhost:1234/AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729/2
Requested item does not exist in pack
```

Under the hood, the pack just stores the payloads and stores offsets in `X-Object-Meta-Pack`:

```
# Item :pack/b33eccfb0e75a2bccdcd7f7fca237729/2 ('CC') was logically deleted before
# hence the empty field in 'X-Object-Meta-Pack' 
$ curl \
  -v \
  -H 'X-Auth-Token: ...' \
  localhost:1234/AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729

< HTTP/1.1 200 OK
< X-Object-Meta-Pack: 0-7,8-22,,25-28,29-30
...
BBBBBBBBEEEEEEEEEEEEEEECCAAAADD
```

Once the repack threshold is reached, the pack is re-uploaded:

```
# Deletes 'EEEEEEEEEEEEEEE'
$ curl \
  -X DELETE \
  -H 'X-Auth-Token: ...' \
  localhost:1234/AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729/2

# Now the pack is re-packed
$ curl \
  -H 'X-Auth-Token: ...' \
  localhost:1234/AUTH_phil/cont1/:pack/b33eccfb0e75a2bccdcd7f7fca237729/4
BBBBBBBBAAAADD
```

## Author & License 
Philipp C. Heckel, Apache License 2.0