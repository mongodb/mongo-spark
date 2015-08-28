+++
date = "2015-08-28T12:56:09-04:00"
draft = true
title = "Client Setup"
[menu.main]
  parent = "Core Reference"
  identifier = "Core Client Setup"
  weight = 70
  pre = "<i class='fa'></i>"
+++

## Client Setup

For a walkthrough of the main connector functionality please refer to the
[Quick Tour]({{< relref "core/getting-started/quick-tour.md" >}}).

### Connection String URI

{{% note %}}
See [Connection String URI Format](https://docs.mongodb.org/v3.0/reference/connection-string/)
for detailed information about formatting and options available through a
connection string URI.
{{% /note %}}

The easiest way to set up a `ClientProvider` is through a connection string URI.

```java
MongoClientProvider clientProvider = new MongoSparkClientProvider(uri);
```

### Advanced Client Options

For advanced client options supported by
[`MongoClientOptions.Builder`](http://api.mongodb.org/java/current/com/mongodb/MongoClientOptions.Builder.html),
more care is required in setting up the client provider. In particular, a
`MongoClientOptionsBuilderInitializer` is necessary to configure the advanced
options. The initializer is essentially a function that supplies a
`MongoClientOptions.Builder` for use in client instantion from a
`ClientProvider`.

Suppose a client needs a custom codec:

```java
MongoClientOptionsBuilderInitializer builderInitializer =
        new MongoSparkClientOptionsBuilderInitializer(() -> {
            CodecRegistry codecRegistry = CodecRegistries.fromCodecs(new MyCodec());
            return new Builder().codecRegistry(codecRegistry);
        });
```

In this example, the function is created but the `Builder` is not instantiated
until the function is called by the `ClientProvider`. Note that the codec is
instantiated *within* the scope of the function.

```java
MyCodec<myType> codec = new MyCodec<>();

MongoClientOptionsBuilderInitializer builderInitializer =
        new MongoSparkClientOptionsBuilderInitializer(() -> {
            // WRONG! java.io.NotSerializableException
            CodecRegistry codecRegistry = CodecRegistries.fromCodecs(codec);
            return new Builder().codecRegistry(codecRegistry);
        });
```

The initializer must be serialized to each worker node, therefore
non-serializable objects outside of the scope of the function may not be
referenced.

At this point, a `ClientProvider` may be set up through a connection string URI
and initializer.

```java
MongoClientProvider clientProvider = new MongoSparkClientProvider(uri, builderInitializer);
```
