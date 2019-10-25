# swift-packer

Packing small objects (concurrent PUTs):
```http request
PUT /v1/AUTH_user1/cont1/small1 HTTP/1.1
X-Allow-Pack: yes
X-Pack-Group: mygroup1
...

HTTP/1.1 200 OK
X-Packed: yes
X-Pack-Path: AUTH_user1/cont1/:pack/beeaffe15abe
X-Item-Path: AUTH_user1/cont1/:pack/beeaffe15abe/0
```

```http request
PUT /v1/AUTH_user2/cont2/small2 HTTP/1.1
X-Allow-Pack: yes
X-Pack-Group: mygroup1
...

HTTP/1.1 200 OK
X-Packed: yes
X-Pack-Path: AUTH_user1/cont1/:pack/beeaffe15abe
X-Item-Path: AUTH_user1/cont1/:pack/beeaffe15abe/1
```

Large objects are forwarded along:
```http request
PUT /v1/AUTH_user3/cont3/large1 HTTP/1.1
X-Allow-Pack: yes
X-Pack-Group: mygroup1
...

HTTP/1.1 200 OK
X-Packed: no
```

Retrieving packed objects: 
```http request
GET /v1/AUTH_user1/cont1/:pack/beeaffe15abe/0 HTTP/1.1
...
```

Deleting packed objects:
```http request
DELETE /v1/AUTH_user1/cont1/:pack/beeaffe15abe/0 HTTP/1.1
...

```

Retrieving the pack object itself: 
```http request
GET /v1/AUTH_user1/cont1/:pack/beeaffe15abe HTTP/1.1
...

HTTP/1.1 200 OK
Content-Length: 800
X-Object-Meta-Pack: 0-201,201-800
...
```