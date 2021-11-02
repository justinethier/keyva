## [Key-value store web API](https://web.eecs.utk.edu/~azh/blog/morechallengingprojects.html)

> A key-value store is really just a dictionary. Give it a key, get back a value. You can add new keys, remove keys, or update values. Viola, you have created a NoSQL database! But you can take it a step further and offer it as a web API, so that all your future web apps can utilize your database service.
> 
> I really like this project because it is really simple to create the basic "database". You can start by using the dictionary data structure that comes with whatever programming language you're using and slap a web API on top of it. But like all these ideas, there is a lot more that can be added: optimizations for high performance, security and multiple users, atomic transactions, data types, batch operations, persistance, failure recovery, and the ability to run it across multiple servers. Soon enough you'll have a billion dollar product like Redis or Amazon DynamoDB.
> 
> Seriously, fire up your code editor and use your favorite language to try this one. I did it with both Go and Racket to get a feel of the differences. It was quite enlightening. The performance was good enough with my Go version on small tests right out of the box.
> 
> Further reading:
> 
> * Key-value database (Wikipedia)
> * B-tree data structure (Wikipedia)
> * Atomicity (Wikipedia)
> * How I built a key value store in Go ([web])(https://medium.com/@naqvi.jafar91/how-i-built-a-key-value-store-in-go-bd89f68062a8)
> * Badger: Fast key-value DB in Go ([GitHub](https://github.com/dgraph-io/badger))
> * https://github.com/gostor/awesome-go-storage
> * If you want to dive deeper with databases: Database Design for Mere Mortals (Amazon)

## [LearnDB - Learn how to build a database](learndb.net)

## [Understanding LSM Trees: What Powers Write-Heavy Databases](https://yetanotherdevblog.com/lsm/)

Lots of great information here, should build a roadmap based on this. Cliff notes:

We now understand how a basic LSM tree storage engine works:

- Writes are stored in an in-memory tree (also known as a memtable). Any supporting data structures (bloom filters and sparse index) are also updated if necessary.
- When this tree becomes too large it is flushed to disk with the keys in sorted order.
- When a read comes in we check the bloom filter. If the bloom filter indicates that the value is not present then we tell the client that the key could not be found. If the bloom filter indicates that the value is present then we begin iterating over our segment files from newest to oldest.
- For each segment file, we check a sparse index and scan the offsets where we expect the key to be found until we find the key. We'll return the value as soon as we find it in a segment file.

