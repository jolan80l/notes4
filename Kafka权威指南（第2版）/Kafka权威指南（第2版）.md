1. 初识Kafka

## 1.1　发布与订阅消息系统

数据（消息）的发送者（发布者）不会直接把消息发送给接收者，这是发布与订阅消息系统的一个特点。发布者以某种方式对消息进行分类，接收者（订阅者）通过订阅它们来接收特定类型的消息。发布与订阅系统一般会有一个broker，也就是发布消息的地方。

真正需要的是一个单一的集中式系统，它可以用来发布通用的数据，并且规模可以随着公司业务的增长而增长。

## 1.2　Kafka登场

Kafka就是为了解决上述问题而设计的一款基于发布与订阅模式的消息系统。

### 1.2.1　消息和批次

Kafka的数据单元被称为消息。消息可以有一个可选的元数据，也就是键。当需要以一种可控的方式将消息写入不同的分区时，需要用到键。最简单的例子就是为键生成一个一致性哈希值，然后用哈希值对主题分区数进行取模，为消息选取分区。这样可以保证具有相同键的消息总是会被写到相同的分区中（前提是分区数量没有发生变化）​。

为了提高效率，消息会被分成批次写入Kafka。批次包含了一组属于同一个主题和分区的消息。如果每一条消息都单独穿行于网络中，那么就会导致大量的网络开销，把消息分成批次传输可以减少网络开销。不过，这需要在时间延迟和吞吐量之间做出权衡：批次越大，单位时间内处理的消息就越多，对单条消息来说，其传输时间就越长。消息批次会被压缩，这样可以提升数据的传输和存储性能，但需要做更多的计算处理。

### 1.2.2　模式

### 1.2.3　主题和分区

Kafka的消息通过主题进行分类。主题就好比数据库的表或文件系统的文件夹。主题可以被分为若干个分区，一个分区就是一个提交日志。

需要注意的是，由于一个主题一般包含几个分区，因此无法在整个主题范围内保证消息的顺序，但可以保证消息在单个分区内是有序的。

![avatar](img/1.png)

生产者创建消息。在其他发布与订阅系统中，生产者可能被称为发布者或写入者。一条消息会被发布到一个特定的主题上。在默认情况下，生产者会把消息均衡地分布到主题的所有分区中。不过，在某些情况下，生产者会把消息直接写入指定的分区，这通常是通过消息键和分区器来实现的。分区器会为键生成一个哈希值，并将其映射到指定的分区，这样可以保证包含同一个键的消息被写入同一个分区。生产者也可以使用自定义的分区器，根据不同的业务规则将消息映射到不同的分区。

消费者读取消息。在其他发布与订阅系统中，消费者可能被称为订阅者或读取者。消费者会订阅一个或多个主题，并按照消息写入分区的顺序读取它们。消费者通过检查消息的偏移量来区分已经读取过的消息。偏移量（不断递增的整数值）是另一种元数据，在创建消息时，Kafka会把它添加到消息里。在给定的分区中，每一条消息的偏移量都是唯一的，越往后消息的偏移量越大（但不一定是严格单调递增）​。消费者会把每一个分区可能的下一个偏移量保存起来（通常保存在Kafka中）​，如果消费者关闭或重启，则其读取状态不会丢失。

消费者可以是消费者群组的一部分，属于同一群组的一个或多个消费者共同读取一个主题。群组可以保证每个分区只被这个群组里的一个消费者读取

消费者与分区之间的映射通常被称为消费者对分区的所有权关系。

通过这种方式，消费者可以读取包含大量消息的主题。而且，如果一个消费者失效，那么群组里的其他消费者可以接管失效消费者的工作。

### 1.2.5　broker和集群

一台单独的Kafka服务器被称为broker。broker会接收来自生产者的消息，为其设置偏移量，并提交到磁盘保存。broker会为消费者提供服务，对读取分区的请求做出响应，并返回已经发布的消息。

broker组成了集群。每个集群都有一个同时充当了集群控制器角色的broker（自动从活动的集群成员中选举出来）​。控制器负责管理工作，包括为broker分配分区和监控broker。在集群中，一个分区从属于一个broker，这个broker被称为分区的首领。一个被分配给其他broker的分区副本（参见图1-7）叫作这个分区的“跟随者”​。分区复制提供了分区的消息冗余，如果一个broker发生故障，则其中的一个跟随者可以接管它的领导权。所有想要发布消息的生产者必须连接到首领，但消费者可以从首领或者跟随者那里读取消息。第7章将详细介绍如何操作集群（包括复制分区）​。

保留消息（在一定期限内）是Kafka的一个重要特性。broker默认的消息保留策略是这样的：要么保留一段时间（如7天）​，要么保留消息总量达到一定的字节数（如1 GB）​。当消息数量达到这些上限时，旧消息就会过期并被删除。

### 1.2.6　多集群

随着broker数量的增加，最好使用多个集群，原因如下。
● 数据类型分离
● 安全需求隔离
● 多数据中心（灾难恢复）

需要注意的是，Kafka的消息复制机制只能在单个集群中而不能在多个集群之间进行。

## 1.3　为什么选择Kafka

### 1.3.1　多个生产者

Kafka可以无缝支持多个生产者，不管客户端消费的是单个主题还是多个主题。所以，它很适用于从多个前端系统收集数据，并以统一的格式对外提供数据。

### 1.3.2　多个消费者

除了支持多个生产者，Kafka也支持多个消费者从一个单独的消息流读取数据，而且消费者之间互不影响。这与其他队列系统不同。在其他队列系统中，消息一旦被一个客户端读取，就无法再被其他客户端读取。多个消费者还可以组成一个群组，它们共享一个消息流，并保证整个群组只处理一次给定的消息。

### 1.3.3　基于磁盘的数据保留

Kafka不仅支持多个消费者，还允许消费者非实时地读取消息，这要归功于Kafka的数据保留特性。消息会被提交到磁盘，并根据设置的保留策略进行保存。每个主题可以设置单独的保留策略，以满足不同消费者的需求。各个主题还可以保留不同数量的消息。消费者可能会因为处理速度慢或突发的流量高峰而无法及时读取消息，在这种情况下，持久化的数据可以保证数据不会丢失

### 1.3.4　伸缩性

为了能够轻松地处理大量数据，Kafka从一开始就被设计成一个具备灵活伸缩性的系统。用户可以在开发阶段使用单个broker，然后再扩展到包含3个broker的小型开发集群。随着数据量不断增长，在部署到生产环境时，集群可以包含上百个broker。对在线集群进行扩展丝毫不影响系统的整体可用性。也就是说，一个包含多个broker的集群，即使个别broker失效，仍然可以持续地为客户端提供服务

### 1.3.5　高性能

### 1.3.6　平台特性

Kafka核心项目还加入了一些流式平台特性，从而使开发人员能够更容易执行一些常见的任务。结构化运行时环境）​，但这些特性会以API和开。

### 1.4　数据生态系统

应用场景：

- 活动跟踪
- 传递消息
- 指标和日志记录
- 提交日志
- 流式处理

# 第2章　安装Kafka

## 2.1　环境配置

### 2.1.1　选择操作系统

### 2.1.2　安装Java

在安装ZooKeeper或Kafka之前，需要有一个Java运行时环境。Kafka和ZooKeeper可以运行在所有基于OpenJDK的Java运行时中，包括OracleJDK。

### 2.1.3　安装ZooKeeper

为了保证高可用，ZooKeeper以集群（被称为群组）的方式运行。由于使用了再均衡算法，建议一个ZooKeeper集群应该包含奇数个节点（比如3个、5个等）​。只有当群组中的大多数节点（也就是所谓的仲裁）处于可用状态时，ZooKeeper才能处理外部请求。也就是说，一个包含3个节点的群组允许1个节点失效，而一个包含5个节点的群组允许2个节点失效。

## 2.2　安装broker

根据时间保留数据是通过检查日志片段文件的最后修改时间来实现的。一般来说，最后修改时间就是日志片段的关闭时间，也就是文件中最后一条消息的时间戳。不过，如果使用管理工具在服务器间移动分区，那么最后修改时间就不准确了，这种误差可能会导致这些分区过多地保留数据。

### 2.6.1　需要多少个broker

一个Kafka集群需要多少个broker取决于以下几个因素。
● 磁盘容量
● 单个broker的复制容量
● CPU
● 网络

目前，建议每个broker的分区副本不超过14 000个，每个集群的分区副本不超过100万个。

在大多数情况下，CPU通常不是主要瓶颈，但如果有很多客户端连接和请求，则CPU可能会成为瓶颈。

### 2.6.2　broker配置

要让一个broker加入集群，只需要修改两个配置参数。首先，所有broker都必须配置相同的zookeeper.connect，这个参数指定了用于保存元数据的ZooKeeper的群组和路径。其次，每个broker都必须为broker.id指定唯一的值。如果两个broker使用相同的broker.id，那么第二个broker将无法启动

### 2.6.3　操作系统调优

对大多数应用程序（特别是那些依赖吞吐量的应用程序）来说，要尽量避免内存交换。内存页和磁盘之间的数据交换对Kafka各方面的性能都有重大影响。Kafka大量使用了系统页面缓存，如果虚拟内存被交换到磁盘，则说明已经没有多余内存可以分配给页面缓存。

一种避免内存交换的方法是不设置任何交换分区。内存交换不是必需的，不过它确实能够在系统发生灾难性错误时提供一些帮助。内存交换可以防止操作系统由于内存不足而突然终止进程。基于上述原因，建议把vm.swappiness参数的值设置得小一些，比如设置为1。这个参数是指虚拟机子系统使用交换分区的权重百分比。要优先考虑减小页面缓存，而不是进行内存交换。

我们可以将vm.dirty_background_ratio设置为小于默认值10的数字。这个参数是指脏页占系统内存的百分比，大多数情况下设置为5就可以了。它不应该被设置为0，因为那样会促使内核频繁地刷新页面，从而降低内核为磁盘写入提供缓冲的能力。

如果要增加被内核进程刷新到磁盘之前的脏页数量，那么可以将vm.dirty_ratio设置为大于20的值（这个数值也是指脏页占系统内存的百分比）​。这个值可设置的范围很广，60~80是一个比较合理的区间。不过修改这个参数会带来一些风险，比如未刷新的磁盘操作的数量会增加，以及同步刷新导致的I/O等待时间会更长。如果将vm.dirty_ratio设置了较高的值，那么建议启用Kafka的复制功能，避免因系统崩溃造成数据丢失。

一个broker如果有很多分区，那么它至少还需要 ( 分区数量 )×(分区大小 / 日志片段大小 ) 个文件描述符来跟踪所有的日志片段。因此，建议将vm.max_map_count设置为一个非常大的数字（基于前面的公式）​。根据环境的不同，将这个值设置为400 000或600 000是没有问题的。另外，建议将vm.overcommit_memory设置为0。0表示内核会根据应用程序来确定空闲内存的数量。如果这个参数被设置为0以外的值，则可能会导致操作系统夺走过多内存，从而减少Kafka的运行内存，这种情况在高摄取率的应用程序中很常见。

有很多种文件系统可供选择，对本地文件系统来说，Ext4（第四代扩展文件系统）和XFS（扩展文件系统）最为常见。XFS是很多Linux发行版默认的文件系统，因为它只需要做少量调优就可以承担大部分的工作负载，表现要优于Ext4。

XFS为Kafka提供了更好的性能，除了文件系统提供的自动调优，不需要再做额外的调优。另外，XFS的批量磁盘写入的效率更高，所有这些组合在一起，提高了整体的I/O吞吐量。

要合理设置挂载点的noatime属性。文件元数据包含3个时间戳：创建时间(ctime)、最后修改时间(mtime)和最后访问时间(atime)。在默认情况下，每次文件被读取后都会更新atime，这会导致大量的磁盘写操作。atime属性的用处并不大，除非应用程序想要知道某个文件在最近一次修改后有没有被访问过（对于这种情况可以使用relatime）​。Kafka没有用到atime这个属性，所以完全可以将其禁用。设置挂载点的noatime属性可以避免更新atime，但又不影响ctime和mtime。当有大量磁盘写入时，通过设置largeio也有助于提升Kafka的效率。

调整Kafka的网络配置与调整其他大部分Web服务器和网络应用程序的网络配置是一样的。首先，可以调整分配给socket读写缓冲区的默认内存和最大内存，这样可以显著提升大流量网络的传输性能。socket读写缓冲区内存对应的参数分别是net.core.wmem_default和net.core.rmem_default，合理的值是131 072（也就是128 KiB）​。读写缓冲区最大内存对应的参数分别是net.core.wmem_max和net.core.rmem_max，合理的值是2 097 152（也就是2 MiB）​。需要注意的是，最大值并不意味着每个socket一定要分配这么大的缓冲空间，只是在必要的情况下才会达到这个上限。

除了配置socket参数，还需要配置TCP socket的读写缓冲区，对应的参数分别是net.ipv4.tcp_wmem和net.ipv4.tcp_rmem。这些参数的值由3个整数组成，使用空格分隔，分别表示最小值、默认值和最大值。最大值不能大于net.core.wmem_max和net.core.rmem_max指定的大小。例如，​“4096 65536 2048000”表示最小值是4 KiB、默认值是64 KiB、最大值是2 MiB

## 2.7　生产环境的注意事项

### 2.7.1　垃圾回收器选项

建议将G1GC作为Kafka的默认垃圾回收器。在应用程序的整个生命周期中，G1GC会根据工作负载进行自我调节，并且停顿时间是恒定的。它可以轻松收集大块堆内存，把堆内存分为若干小块区域，并不是每次都回收整个堆空间。

MaxGCPauseMillis：该参数指定了每次垃圾回收的默认停顿时间。这个值不是固定的，G1GC可以根据具体情况进行调整，默认是200毫秒。G1GC可以决定垃圾回收的频率，以及每一轮需要回收多少个区域，这样算下来，每一轮垃圾回收大概需要200毫秒。

InitiatingHeapOccupancyPercent：该参数指定了在G1GC启动新一轮垃圾回收之前可以使用的堆内存百分比，默认是45。也就是说，在堆内存使用率达到45%之前，G1GC不会启动垃圾回收。这个百分比包括新生代和老年代的内存。

Kafka使用堆内存以及处理垃圾对象的效率是比较高的，所以可以把这些参数设置得小一些。如果一台服务器有64 GB内存，并使用5 GB堆内存来运行Kafka，那么可以将MaxGCPauseMillis设置为20毫秒，将InitiatingHeapOccupancyPercent设置为35，让垃圾回收比默认的早一些启动。Kafka是在G1GC之前发布的，因此，它默认使用的是CMS（并发标记和清除）垃圾回收器，以确保与所有的JVM兼容。

### 2.7.2　数据中心布局

Kafka可以将新创建的分区分配给部署在不同机架上的broker（机架感知）​，确保单个分区的副本不会都位于同一个机架。

总的来说，最好把集群的broker安装在不同的机架上，至少不要让它们共享可能出现单点故障的基础设施，比如电源和网络。也就是说，部署服务器需要至少两个电源连接（两个不同的回路）和两个网络交换器（保证可以进行无缝的故障切换）​。除了使用两个电源连接，最好把broker安装在不同的机架上。

### 2.7.3　共享ZooKeeper

Kafka使用ZooKeeper保存broker、主题和分区的元数据。只有当消费者群组成员或Kafka集群本身发生变化时才会向ZooKeeper写入数据。这些流量通常很小，所以没有必要为单个Kafka集群使用专门的ZooKeeper群组。实际上，有很多Kafka部署环境使用单个ZooKeeper群组来保存多个Kafka集群的元数据（正如本章之前所描述的那样，每个Kafka集群使用一个单独的chroot路径）​。

- Kafka 2.8.0（2021年）：首次引入 KRaft 模式（早期测试版，不推荐生产环境）。
- Kafka 3.0+（2022年）：KRaft 进入**生产就绪（Production-Ready）**阶段，但仍需谨慎评估。
- Kafka 3.3+（2023年）：进一步优化稳定性与性能，社区推荐新集群直接使用 KRaft。
- Kafka 3.5+（最新版本）：KRaft 已成为默认模式，ZooKeeper 模式逐步进入弃用流程。


# 第3章　Kafka生产者——向Kafka写入数据

多样的应用场景意味着多样的需求：是否每条消息都很重要？是否允许丢失一小部分消息？是否可以接受偶尔出现重复消息？是否有严格的延迟和吞吐量需求？不同的应用场景直接影响如何使用和配置生产者API。

先从创建一个ProducerRecord对象开始，其中需要包含目标主题和要发送的内容。另外，还可以指定键、分区、时间戳或标头。在发送ProducerRecord对象时，生产者需要先把键和值对象序列化成字节数组，这样才能在网络上传输。

接下来，如果没有显式地指定分区，那么数据将被传给分区器。分区器通常会基于ProducerRecord对象的键选择一个分区。选好分区以后，生产者就知道该往哪个主题和分区发送这条消息了。紧接着，该消息会被添加到一个消息批次里，这个批次里的所有消息都将被发送给同一个主题和分区。有一个独立的线程负责把这些消息批次发送给目标broker。

broker在收到这些消息时会返回一个响应。如果消息写入成功，就返回一个RecordMetaData对象，其中包含了主题和分区信息，以及消息在分区中的偏移量。如果消息写入失败，则会返回一个错误。生产者在收到错误之后会尝试重新发送消息，重试几次之后如果还是失败，则会放弃重试，并返回错误信息。

## 3.2　创建Kafka生产者

Kafka生产者有3个必须设置的属性。

- key.serializer：broker的地址。可以由多个host:port组成，生产者用它们来建立初始的Kafka集群连接。它不需要包含所有的broker地址，因为生产者在建立初始连接之后可以从给定的broker那里找到其他broker的信息。不过还是建议至少提供两个broker地址，因为一旦其中一个停机，则生产者仍然可以连接到集群。

- key.serializer：一个类名，用来序列化消息的键。broker希望接收到的消息的键和值都是字节数组。生产者可以把任意Java对象作为键和值发送给broker，但它需要知道如何把这些Java对象转换成字节数组。key.serializer必须被设置为一个实现了org.apache.kafka.common.serialization.Serializer接口的类，生产者会用这个类把键序列化成字节数组。Kafka客户端默认提供了ByteArraySerializer、StringSerializer和IntegerSerializer等，如果你只使用常见的几种Java对象类型，就没有必要实现自己的序列化器。需要注意的是，必须设置key.serializer这个属性，尽管你可能只需要将值发送给Kafka。如果只需要发送值，则可以将Void作为键的类型，然后将这个属性设置为VoidSerializer。

- value.serializer：一个类名，用来序列化消息的值。与设置key.serializer属性一样，需要将value.serializer设置成可以序列化消息值对象的类。

```java
Properties kafkaProps = new Properties(); ➊
kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
kafkaProps.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"); ➋
kafkaProps.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer");

producer = new KafkaProducer<String, String>(kafkaProps); ➌
```

发送消息主要有以下3种方式。

- 发送并忘记：把消息发送给服务器，但并不关心它是否成功送达。大多数情况下，消息可以成功送达，因为Kafka是高可用的，而且生产者有自动尝试重发的机制。但是，如果发生了不可重试的错误或超时，那么消息将会丢失，应用程序将不会收到任何信息或异常。

- 同步发送：一般来说，生产者是异步的——我们调用send()方法发送消息，它会返回一个Future对象。可以调用get()方法等待Future完成，这样就可以在发送下一条消息之前知道当前消息是否发送成功。

- 异步发送：调用send()方法，并指定一个回调函数，当服务器返回响应时，这个函数会被触发。

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("CustomerCountry", "Precision Products",
        "France"); ➊
try {
    producer.send(record); ➋
} catch (Exception e) {
    e.printStackTrace(); ➌
}
```

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
try {
    producer.send(record).get(); ➊
} catch (Exception e) {
    e.printStackTrace(); ➋
}
```

```java
private class DemoProducerCallback implements Callback { ➊
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace(); ➋
        }
    }
}

ProducerRecord<String, String> record =
    new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA"); ➌
producer.send(record, new DemoProducerCallback()); ➍
```

## 3.4　生产者配置

### 3.4.1　client.id

client.id是客户端标识符，它的值可以是任意字符串，broker用它来识别从客户端发送过来的消息。选择一个好的客户端标识符可以让故障诊断变得更容易些.

### 3.4.2　acks

acks指定了生产者在多少个分区副本收到消息的情况下才会认为消息写入成功。在默认情况下，Kafka会在首领副本收到消息后向客户端回应消息写入成功（Kafka 3.0预计会改变这个默认行为）​。这个参数对写入消息的持久性有重大影响，对于不同的场景，使用默认值可能不是最好的选择。

- acks=0: 如果acks=0，则生产者不会等待任何来自broker的响应。也就是说，如果broker因为某些问题没有收到消息，那么生产者便无从得知，消息也就丢失了。不过，因为生产者不需要等待broker返回响应，所以它们能够以网络可支持的最大速度发送消息，从而达到很高的吞吐量。

- acks=1: 如果acks=1，那么只要集群的首领副本收到消息，生产者就会收到消息成功写入的响应。如果消息无法到达首领副本（比如首领副本发生崩溃，新首领还未选举出来）​，那么生产者会收到一个错误响应。为了避免数据丢失，生产者会尝试重发消息。不过，在首领副本发生崩溃的情况下，如果消息还没有被复制到新的首领副本，则消息还是有可能丢失。

- acks=all: 如果acks=all，那么只有当所有副本全部收到消息时，生产者才会收到消息成功写入的响应。这种模式是最安全的，它可以保证不止一个broker收到消息，就算有个别broker发生崩溃，整个集群仍然可以运行（第6章将讨论更多的细节）​。不过，它的延迟比acks=1高，因为生产者需要等待不止一个broker确认收到消息。

### 3.4.3　消息传递时间

从Kafka 2.1开始，我们将ProduceRecord的发送时间分成如下两个时间间隔，它们是被分开处理的。

● 异步调用send()所花费的时间。在此期间，调用send()的线程将被阻塞。
● 从异步调用send()返回到触发回调（不管是成功还是失败）的时间，也就是从ProduceRecord被放到批次中直到Kafka成功响应、出现不可恢复异常或发送超时的时间。

- max.block.ms：这个参数用于控制在调用send()或通过partitionsFor()显式地请求元数据时生产者可以发生阻塞的时间。当生产者的发送缓冲区被填满或元数据不可用时，这些方法就可能发生阻塞。当达到max.block.ms配置的时间时，就会抛出一个超时异常。

- delivery.timeout.ms这个参数用于控制从消息准备好发送（send()方法成功返回并将消息放入批次中）到broker响应或客户端放弃发送（包括重试）所花费的时间。如图3-2所示，这个时间应该大于linger.ms和request.timeout.ms。如果配置的时间不满足这一点，则会抛出异常。通常，成功发送消息的速度要比delivery.timeout.ms快得多。一般建议这个时间大于broker重新选举的时间30s，建议可以配置为120。

- request.timeout.ms这个参数用于控制生产者在发送消息时等待服务器响应的时间。需要注意的是，这是指生产者在放弃之前等待每个请求的时间，不包括重试、发送之前所花费的时间等。如果设置的值已触及，但服务器没有响应，那么生产者将重试发送，或者执行回调，并传给它一个TimeoutException。

- retries 和retry.backoff.ms当生产者收到来自服务器的错误消息时，这个错误有可能是暂时的（例如，一个分区没有首领）​。在这种情况下，retries参数可用于控制生产者在放弃发送并向客户端宣告失败之前可以重试多少次。在默认情况下，重试时间间隔是100毫秒，但可以通过retry.backoff.ms参数来控制重试时间间隔。

生产者并不会重试所有的错误。有些错误不是暂时的，生产者就不会进行重试（例如，​“消息太大”错误）​。通常，对于可重试的错误，生产者会自动进行重试，所以不需要在应用程序中处理重试逻辑。你要做的是集中精力处理不可重试的错误或者当重试次数达到上限时的情况。

### 3.4.4　linger.ms

这个参数指定了生产者在发送消息批次之前等待更多消息加入批次的时间。生产者会在批次被填满或等待时间达到linger.ms时把消息批次发送出去。在默认情况下，只要有可用的发送者线程，生产者都会直接把批次发送出去，就算批次中只有一条消息。把linger.ms设置成比0大的数，可以让生产者在将批次发送给服务器之前等待一会儿，以使更多的消息加入批次中。虽然这样会增加一点儿延迟，但也极大地提升了吞吐量。这是因为一次性发送的消息越多，每条消息的开销就越小，如果启用了压缩，则计算量也更少了。

### 3.4.5　buffer.memory

这个参数用来设置生产者要发送给服务器的消息的内存缓冲区大小。如果应用程序调用send()方法的速度超过生产者将消息发送给服务器的速度，那么生产者的缓冲空间可能会被耗尽，后续的send()方法调用会等待内存空间被释放，如果在max.block.ms之后还没有可用空间，就抛出异常。需要注意的是，这个异常与其他异常不一样，它是send()方法而不是Future对象抛出来的。

### 3.4.6　compression.type

在默认情况下，生产者发送的消息是未经压缩的。这个参数可以被设置为snappy、gzip、lz4或zstd，这指定了消息被发送给broker之前使用哪一种压缩算法。snappy压缩算法由谷歌发明，虽然占用较少的CPU时间，但能提供较好的性能和相当可观的压缩比。如果同时有性能和网络带宽方面的考虑，那么可以使用这种算法。gzip压缩算法通常会占用较多的CPU时间，但提供了更高的压缩比。如果网络带宽比较有限，则可以使用这种算法。使用压缩可以降低网络传输和存储开销，而这些往往是向Kafka发送消息的瓶颈所在。

### 3.4.7　batch.size

当有多条消息被发送给同一个分区时，生产者会把它们放在同一个批次里。这个参数指定了一个批次可以使用的内存大小。需要注意的是，该参数是按照字节数而不是消息条数来计算的。当批次被填满时，批次里所有的消息都将被发送出去。但是生产者并不一定都会等到批次被填满时才将其发送出去。那些未填满的批次，甚至只包含一条消息的批次也有可能被发送出去。所以，就算把批次大小设置得很大，也不会导致延迟，只是会占用更多的内存而已。但如果把批次大小设置得太小，则会增加一些额外的开销，因为生产者需要更频繁地发送消息。

### 3.4.8　max.in.flight.requests.per.connection

这个参数指定了生产者在收到服务器响应之前可以发送多少个消息批次。它的值越大，占用的内存就越多，不过吞吐量也会得到提升。在单数据中心环境中，该参数被设置为2时可以获得最佳的吞吐量，但使用默认值5也可以获得差不多的性能。

Kafka可以保证同一个分区中的消息是有序的。也就是说，如果生产者按照一定的顺序发送消息，那么broker会按照这个顺序把它们写入分区，消费者也会按照同样的顺序读取它们。在某些情况下，顺序是非常重要的。

### 3.4.9　max.request.size

这个参数用于控制生产者发送的请求的大小。它限制了可发送的单条最大消息的大小和单个请求的消息总量的大小。另外，broker对可接收的最大消息也有限制(message.max.bytes)，其两边的配置最好是匹配的，以免生产者发送的消息被broker拒绝。

### 3.4.10　receive.buffer.bytes和send.buffer.bytes

这两个参数分别指定了TCP socket接收和发送数据包的缓冲区大小。如果它们被设为–1，就使用操作系统默认值。如果生产者或消费者与broker位于不同的数据中心，则可以适当加大它们的值，因为跨数据中心网络的延迟一般都比较高，而带宽又比较低。

### 3.4.11　enable.idempotence

为了避免broker出现重复的消息，可以将enable.idempotence设置为true。当幂等生产者被启用时，生产者将给发送的每一条消息都加上一个序列号。如果broker收到具有相同序列号的消息，那么它就会拒绝第二个副本，而生产者则会收到DuplicateSequenceException，这个异常对生产者来说是无害的。

如果要启用幂等性，那么max.in.flight.requests.per.connection应小于或等于5、retries应大于0，并且acks被设置为all。如果设置了不恰当的值，则会抛出ConfigException异常。

## 3.5　序列化器

### 3.5.1　自定义序列化器

如果要发送给Kafka的对象不是简单的字符串或整型，则既可以用通用的序列化框架（比如Avro、Thrift或Protobuf）来创建消息，也可以使用自定义序列化器。强烈建议使用通用的序列化框架。

## 3.6　分区

在上面的例子中，ProducerRecord对象包含了主题名称、记录的键和值。Kafka消息就是一个个的键–值对，ProducerRecord对象可以只包含主题名称和值，键默认情况下是null。不过，大多数应用程序还是会用键来发送消息。键有两种用途：一是作为消息的附加信息与消息保存在一起，二是用来确定消息应该被写入主题的哪个分区（键在压缩主题中也扮演了重要角色），具有相同键的消息将被写入同一个分区。如果一个进程只从主题的某些分区读取数据，那么具有相同键的所有记录都会被这个进程读取。

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("CustomerCountry", "Laboratory Equipment", "USA");
```

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("CustomerCountry", "USA"); //这里的键将被设置为null。
```

如果键为null，并且使用了默认的分区器，那么记录将被随机发送给主题的分区。分区器使用轮询调度(round-robin)算法将消息均衡地分布到各个分区中。从Kafka 2.4开始，在处理键为null的记录时，默认分区器使用的轮询调度算法具备了黏性。也就是说，在切换到下一个分区之前，它会将同一个批次的消息全部写入当前分区。这样就可以使用更少的请求发送相同数量的消息，既降低了延迟，又减少了broker占用CPU的时间。

如果键不为空且使用了默认的分区器，那么Kafka会对键进行哈希（使用Kafka自己的哈希算法，即使升级Java版本，哈希值也不会发生变化）​，然后根据哈希值把消息映射到特定的分区。这里的关键在于同一个键总是被映射到同一个分区，所以在进行映射时，会用到主题所有的分区，而不只是可用的分区。这也意味着，如果在写入数据时目标分区不可用，那么就会出错。不过这种情况很少发生。

除了默认的分区器，Kafka客户端还提供了RoundRobinPartitioner和UniformStickyPartitioner。在消息包含键的情况下，可以用它们来实现随机分区分配和黏性随机分区分配。也可以用UniformStickyPartitioner将负载均衡地分布到所有分区。

如果使用了默认的分区器，那么只有在不改变主题分区数量的情况下键与分区之间的映射才能保持一致。

除了哈希分区，有时也需要使用不一样的分区策略。假设你是B2B供应商，你有一个大客户，它是手持设备Banana的制造商。你的日常交易中有10%以上的交易与这个客户有关。如果使用默认的哈希分区算法，那么与Banana相关的记录就会和其他客户的记录一起被分配给相同的分区，导致这个分区比其他分区大很多。服务器可能会出现存储空间不足、请求处理缓慢等问题。因此，需要给Banana分配单独的分区，然后使用哈希分区算法将其他记录分配给其他分区。

```java
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

public class BananaPartitioner implements Partitioner {

    public void configure(Map<String, ?> configs) {} ➊

    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes,
                         Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if ((keyBytes == null) || (!(key instanceOf String))) ➋
            throw new InvalidRecordException("We expect all messages "+
                "to have customer name as key");

        if (((String) key).equals("Banana"))
            return numPartitions - 1; // Banana的记录总是被分配到最后一个分区

        // 其他记录被哈希到其他分区
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    public void close() {}
}
```

## 3.7　标头

除了键和值，记录还可以包含标头(可以理解为报文头信息)。可以在不改变记录键–值对的情况下向标头中添加一些有关记录的元数据。标头指明了记录数据的来源，可以在不解析消息体的情况下根据标头信息来路由或跟踪消息（消息有可能被加密，而路由器没有访问加密数据的权限）​。

标头由一系列有序的键–值对组成。键是字符串，值可以是任意被序列化的对象，就像消息里的值一样。

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("CustomerCountry", "Precision Products", "France");

record.headers().add("privacy-level","YOLO".getBytes(StandardCharsets.UTF_8));
```

## 3.8　拦截器

有时候，你希望在不修改代码的情况下改变Kafka客户端的行为。这或许是因为你想给公司所有的应用程序都加上同样的行为，或许是因为无法访问应用程序的原始代码。Kafka的ProducerInterceptor拦截器包含两个关键方法。ProducerRecord<K, V> onSend(ProducerRecord<K, V> record)

有时候，你希望在不修改代码的情况下改变Kafka客户端的行为。这或许是因为你想给公司所有的应用程序都加上同样的行为，或许是因为无法访问应用程序的原始代码。Kafka的ProducerInterceptor拦截器包含两个关键方法。ProducerRecord<K, V> onSend(ProducerRecord<K, V> record)。

void onAcknowledgement(RecordMetadata metadata, Exceptionexception) 这个方法会在收到Kafka的确认响应时调用。如果覆盖了这个方法，则不可以修改Kafka返回的响应，但可以捕获到有关响应的信息。常见的生产者拦截器应用场景包括：捕获监控和跟踪信息、为消息添加标头，以及敏感信息脱敏。

```java
public class CountingProducerInterceptor implements ProducerInterceptor {

  ScheduledExecutorService executorService =
          Executors.newSingleThreadScheduledExecutor();
  static AtomicLong numSent = new AtomicLong(0);
  static AtomicLong numAcked = new AtomicLong(0);

  public void configure(Map<String, ?> map) {
      Long windowSize = Long.valueOf(
              (String) map.get("counting.interceptor.window.size.ms")); ➊
      executorService.scheduleAtFixedRate(CountingProducerInterceptor::run,
              windowSize, windowSize, TimeUnit.MILLISECONDS);
  }

  public ProducerRecord onSend(ProducerRecord producerRecord) {
      numSent.incrementAndGet();
      return producerRecord; ➋
  }

  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
      numAcked.incrementAndGet(); ➌
  }

  public void close() {
      executorService.shutdownNow(); ➍
  }

  public static void run() {
      System.out.println(numSent.getAndSet(0));
      System.out.println(numAcked.getAndSet(0));
  }
}
```
## 3.9　配额和节流

Kafka可以限制生产消息和消费消息的速率，这是通过配额机制来实现的。Kafka提供了3种配额类型：生产、消费和请求。生产配额和消费配额限制了客户端发送和接收数据的速率（以字节 / 秒为单位）​。请求配额限制了broker用于处理客户端请求的时间百分比。

可以为所有客户端（使用默认配额）​、特定客户端、特定用户，或特定客户端及特定用户设置配额。特定用户的配额只在集群配置了安全特性并对客户端进行了身份验证后才有效。

默认的生产配额和消费配额是broker配置文件的一部分。如果要限制每个生产者平均发送的消息不超过2 MBps，那么可以在broker配置文件中加入quota.producer.default=2M。

也可以覆盖broker配置文件中的默认配额来为某些客户端配置特定的配额，尽管不建议这么做。如果允许clientA的配额达到4 MBps、clientB的配额达到10 MBps，则可以这样配置：quota.producer.override="clientA:4M,clientB:10M"。

特定客户端的配额通常采用动态配置。可以用kafkaconfig.sh或AdminClient API来动态设置配额。

```bash
bin/kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'producer_
byte_rate=1024' --entity-name clientC --entity-type clients ➊

bin/kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'producer_
byte_rate=1024,consumer_byte_rate=2048' --entity-name user1 --entity-type users ➋

bin/kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'consumer_
byte_rate=2048' --entity-type users ➌
```

❶ 限制clientC（通过客户端ID来识别）每秒平均发送不超过1024字节。❷ 限制user1（通过已认证的账号来识别）每秒平均发送不超过1024字节以及每秒平均消费不超过2048字节。 ❸ 限制所有用户每秒平均消费不超过2048字节，有覆盖配置的特定用户除外。这也是动态修改默认配置的一种方式。

当客户端触及配额时，broker会开始限制客户端请求，以防止超出配额。这意味着broker将延迟对客户端请求做出响应。对大多数客户端来说，这样会自动降低请求速率（因为执行中的请求数量也是有限制的）​，并将客户端流量降到配额允许的范围内。但是，被节流的客户端还是有可能向服务器端发送额外的请求，为了不受影响，broker将在一段时间内暂停与客户端之间的通信通道，以满足配额要求。

节流行为通过produce-throttle-time-avg、produce-throttle-time-max、fetch-throttle-time-avg和fetch-throttle-time-max暴露给客户端，这几个参数是生产请求和消费请求因节流而被延迟的平均时间和最长时间

如果你异步调用Producer.send()，并且发送速率超过了broker能够接受的速率（无论是由于配额的限制还是由于处理能力不足）​，那么消息将会被放入客户端的内存队列。如果发送速率一直快于接收速率，那么客户端最终将耗尽内存缓冲区，并阻塞后续的Producer.send()调用。如果超时延迟不足以让broker赶上生产者，使其清理掉一些缓冲区空间，那么Producer.send()最终将抛出TimeoutException异常。或者，批次里的记录因为等待时间超过了delivery.timeout.ms而过期，导致执行send()的回调，并抛出TimeoutException异常。因此，要做好计划和监控，确保broker的处理能力总是与生产者发送数据的速率相匹配。

# 第4章　Kafka消费者——从Kafka读取数据

## 4.1　Kafka消费者相关概念

### 4.1.1　消费者和消费者群组

就像多个生产者可以向相同的主题写入消息一样，也可以让多个消费者从同一个主题读取消息。Kafka消费者从属于消费者群组。一个群组里的消费者订阅的是同一个主题，每个消费者负责读取这个主题的部分消息。

假设主题T1有4个分区，我们创建了消费者C1，它是群组G1中唯一的消费者，用于订阅主题T1。消费者C1将收到主题T1全部4个分区的消息。

![avatar](img/3.png)

如果在群组G1里新增一个消费者C2，那么每个消费者将接收到两个分区的消息。假设消费者C1接收分区0和分区2的消息，消费者C2接收分区1和分区3的消息。

![avatar](img/4.png)

如果群组G1有4个消费者，那么每个消费者将可以分配到一个分区。

![avatar](img/5.png)

如果向群组里添加更多的消费者，以致超过了主题的分区数量，那么就会有一部分消费者处于空闲状态，不会接收到任何消息。

![avatar](img/7.png)

向群组里添加消费者是横向扩展数据处理能力的主要方式。Kafka消费者经常需要执行一些高延迟的操作，比如把数据写到数据库或用数据做一些比较耗时的计算。在这些情况下，单个消费者无法跟上数据生成的速度，因此可以增加更多的消费者来分担负载，让每个消费者只处理部分分区的消息，这是横向扩展消费者的主要方式。于是，我们可以为主题创建大量的分区，当负载急剧增长时，可以加入更多的消费者。不过需要注意的是，不要让消费者的数量超过主题分区的数量，因为多余的消费者只会被闲置。

除了通过增加消费者数量来横向伸缩单个应用程序，我们还经常遇到多个应用程序从同一个主题读取数据的情况。实际上，Kafka的一个主要设计目标是让Kafka主题里的数据能够满足企业各种应用场景的需求。在这些应用场景中，我们希望每一个应用程序都能获取到所有的消息，而不只是其中的一部分。只要保证每个应用程序都有自己的消费者群组就可以让它们获取到所有的消息。不同于传统的消息系统，横向伸缩消费者和消费者群组并不会导致Kafka性能下降。在之前的例子中，如果新增一个只包含一个消费者的群组G2，那么这个消费者将接收到主题T1的所有消息，与群组G1之间互不影响。群组G2可以增加更多的消费者，每个消费者会读取若干个分区，就像群组G1里的消费者那样。作为整体来说，群组G2还是会收到所有消息，不管有没有其他群组存在。

![avatar](img/8.png)

### 4.1.2　消费者群组和分区再均衡

消费者群组里的消费者共享主题分区的所有权。当一个新消费者加入群组时，它将开始读取一部分原本由其他消费者读取的消息。当一个消费者被关闭或发生崩溃时，它将离开群组，原本由它读取的分区将由群组里的其他消费者读取。主题发生变化（比如管理员添加了新分区）会导致分区重分配。

分区的所有权从一个消费者转移到另一个消费者的行为称为再均衡。再均衡非常重要，它为消费者群组带来了高可用性和伸缩性（你可以放心地添加或移除消费者）​。不过，在正常情况下，我们并不希望发生再均衡。

__主动再均衡__

在进行主动再均衡期间，所有消费者都会停止读取消息，放弃分区所有权，重新加入消费者群组，并获得重新分配到的分区。这样会导致整个消费者群组在一个很短的时间窗口内不可用。主动再均衡包含两个不同的阶段：第一个阶段，所有消费者都放弃分区所有权；第二个阶段，消费者重新加入群组，获得重新分配到的分区，并继续读取消息。

![avatar](img/9.png)

__协作再均衡__

协作再均衡（也称为增量再均衡）通常是指将一个消费者的部分分区重新分配给另一个消费者，其他消费者则继续读取没有被重新分配的分区。这种再均衡包含两个或多个阶段。

在第一个阶段，消费者群组首领会通知所有消费者，它们将失去部分分区的所有权，然后消费者会停止读取这些分区，并放弃对它们的所有权。在第二个阶段，消费者群组首领会将这些没有所有权的分区分配给其他消费者。虽然这种增量再均衡可能需要进行几次迭代，直到达到稳定状态，但它避免了主动再均衡中出现的“停止世界”停顿。

![avatar](img/10.png)

消费者会向被指定为群组协调器的broker（不同消费者群组的协调器可能不同）发送心跳，以此来保持群组成员关系和对分区的所有权关系。心跳是由消费者的一个后台线程发送的，只要消费者能够以正常的时间间隔发送心跳，它就会被认为还“活着”​。如果消费者在足够长的一段时间内没有发送心跳，那么它的会话就将超时，群组协调器会认为它已经“死亡”​，进而触发再均衡。

### 4.1.3　群组固定成员

在默认情况下，消费者的群组成员身份标识是临时的。当一个消费者离开群组时，分配给它的分区所有权将被撤销；当该消费者重新加入时，将通过再均衡协议为其分配一个新的成员ID和新分区。可以给消费者分配一个唯一的group.instance.id，让它成为群组的固定成员。通常，当消费者第一次以固定成员身份加入群组时，群组协调器会按照分区分配策略给它分配一部分分区。当这个消费者被关闭时，它不会自动离开群组——它仍然是群组的成员，直到会话超时。当这个消费者重新加入群组时，它会继续持有之前的身份，并分配到之前所持有的分区。群组协调器缓存了每个成员的分区分配信息，只需要将缓存中的信息发送给重新加入的固定成员，不需要进行再均衡。

如果应用程序需要维护与消费者分区所有权相关的本地状态或缓存，那么群组固定成员关系就非常有用。如果重建本地缓存非常耗时，那么你肯定不希望在每次重启消费者时都经历这个过程。更重要的是，在消费者重启时，消费者所拥有的分区不会被重新分配。在重启过程中，消费者不会读取这些分区，所以当消费者重启完毕时，读取进度会稍稍落后，但你要相信它们一定会赶上。需要注意的是，群组的固定成员在关闭时不会主动离开群组，它们何时“真正消失”取决于session.timeout.ms参数。你可以将这个参数设置得足够大，避免在进行简单的应用程序重启时触发再均衡，但又要设置得足够小，以便在出现严重停机时自动重新分配分区，避免这些分区的读取进度出现较大的滞后。

## 4.2　创建Kafka消费者

在读取消息之前，需要先创建一个KafkaConsumer对象。创建KafkaConsumer对象与创建KafkaProducer对象非常相似——把想要传给消费者的属性放在Properties对象里。本章后续部分将深入介绍所有的配置属性。这里只提供3个必要的属性：bootstrap.servers、key.deserializer和value.deserializer。

第一个属性bootstrap.servers指定了连接Kafka集群的字符串。另外两个属性key.deserializer和value.deserializer，把字节数组转成Java对象。

第4个属性group.id不是必需的，但会经常被用到。它指定了一个消费者属于哪一个消费者群组。也可以创建不属于任何一个群组的消费者，只是这种做法不太常见，所以本书的大部分章节会假设消费者属于某个群组。

```java
Properties props = new Properties();
props.put("bootstrap.servers", "broker1:9092,broker2:9092");
props.put("group.id", "CountryCounter");
props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumer<String, String> consumer =
    new KafkaConsumer<String,String>(props);
```

## 4.3　订阅主题

在创建好消费者之后，下一步就可以开始订阅主题了。subscribe()方法会接收一个主题列表作为参数。

```java
consumer.subscribe(Collections.singletonList("customerCountries")); 
```

也可以在调用subscribe()方法时传入一个正则表达式。正则表达式可以匹配多个主题，如果有人创建了新主题，并且主题的名字与正则表达式匹配，那么就会立即触发一次再均衡，然后消费者就可以读取新主题里的消息。比如要订阅所有与测试相关的主题，那么可以这样做。

```java
consumer.subscribe(Pattern.compile("test.*"));
```

如果你的Kafka集群包含了大量分区（比如30 000个或更多）​，则需注意，主题的过滤是在客户端完成的。当你使用正则表达式而不是指定列表订阅主题时，消费者将定期向broker请求所有已订阅的主题及分区。然后，客户端会用这个列表来检查是否有新增的主题，如果有，就订阅它们。如果主题很多，消费者也很多，那么通过正则表达式订阅主题就会给broker、客户端和网络带来很大的开销。在某些情况下，主题元数据使用的带宽会超过用于发送数据的带宽。另外，为了能够使用正则表达式订阅主题，需要授予客户端获取集群全部主题元数据的权限，即全面描述整个集群的权限。

## 4.4　轮询

消费者API最核心的东西是通过一个简单的轮询向服务器请求数据.

```java
Duration timeout = Duration.ofMillis(100);

while (true) { ➊
    ConsumerRecords<String, String> records = consumer.poll(timeout); ➋

    for (ConsumerRecord<String, String> record : records) { ➌
        System.out.printf("topic = %s, partition = %d, offset = %d, " +
                        "customer = %s, country = %s\n",
        record.topic(), record.partition(), record.offset(),
                record.key(), record.value());
        int updatedCount = 1;
        if (custCountryMap.containsKey(record.value())) {
            updatedCount = custCountryMap.get(record.value()) + 1;
        }
        custCountryMap.put(record.value(), updatedCount);

        JSONObject json = new JSONObject(custCountryMap);
        System.out.println(json.toString()); ➍
    }
}
```
❷ 这一行代码非常重要。消费者必须持续对Kafka进行轮询，否则会被认为已经“死亡”​，它所消费的分区将被移交给群组里其他的消费者。传给poll()的参数是一个超时时间间隔，用于控制poll()的阻塞时间（当消费者缓冲区里没有可用数据时会发生阻塞）​。如果这个参数被设置为0或者有可用的数据，那么poll()就会立即返回，否则它会等待指定的毫秒数。

❸ poll()方法会返回一个记录列表。列表中的每一条记录都包含了主题和分区的信息、记录在分区里的偏移量，以及记录的键–值对。我们一般会遍历这个列表，逐条处理记录。

轮询不只是获取数据那么简单。在第一次调用消费者的poll()方法时，它需要找到GroupCoordinator，加入群组，并接收分配给它的分区。如果触发了再均衡，则整个再均衡过程也会在轮询里进行，包括执行相关的回调

需要注意的是，如果超过max.poll.interval.ms没有调用poll()，则消费者将被认为已经“死亡”​，并被逐出消费者群组。因此，要避免在轮询循环中做任何可能导致不可预知的阻塞的操作。

__线程安全__

按照规则，一个消费者使用一个线程。如果要在应用程序的同一个消费者群组里运行多个消费者，则需要让每个消费者运行在自己的线程中。最好是把消费者的逻辑封装在自己的对象里，然后用Java的ExecutorService启动多个线程，让每个消费者运行在自己的线程中。

在旧版本Kafka中，轮询方法的完整签名是poll(long)。现在，这个签名被弃用了，新API的签名是poll(Duration)。除了参数类型发生变化，方法体里的阻塞语义也发生了细微的改变。原来的方法会一直阻塞，直到从Kafka获取所需的元数据，即使阻塞时间比指定的超时时间还长。新方法将遵守超时限制，不会一直等待元数据返回。如果你已经有一个消费者使用poll(0) 来获取Kafka元数据（不消费任何记录，这是一种相当常见的做法）​，那么就不要指望把它改成poll(Duration.ofMillis(0)) 后还能获得同样的效果。你需要想新的办法来达到目的。通常的解决办法是将逻辑放在rebalanceListener.onPartitionAssignment()方法里，这个方法一定会在获取分区元数据之后以及记录开始到达之前被调用。

## 4.5　配置消费者

### 4.5.1　fetch.min.bytes

这个属性指定了消费者从服务器获取记录的最小字节数，默认是1字节。broker在收到消费者的获取数据请求时，如果可用数据量小于fetch.min.bytes指定的大小，那么它就会等到有足够可用数据时才将数据返回。这样可以降低消费者和broker的负载，因为它们在主题流量不是很大的时候（或者一天里的低流量时段）不需要来来回回地传输消息。如果消费者在没有太多可用数据时CPU使用率很高，或者在有很多消费者时为了降低broker的负载，那么可以把这个属性的值设置 得比默认值大。但需要注意的是，在低吞吐量的情况下，加大这个值会增加延迟。

### 4.5.2　fetch.max.wait.ms

通过设置fetch.min.bytes，可以让Kafka等到有足够多的数据时才将它们返回给消费者，feth.max.wait.ms则用于指定broker等待的时间，默认是500毫秒。如果没有足够多的数据流入Kafka，那么消费者获取数据的请求就得不到满足，最多会导致500毫秒的延迟。如果要降低潜在的延迟（为了满足SLA）​，那么可以把这个属性的值设置得小一些。如果fetch.max.wait.ms被设置为100毫秒，fetch.min.bytes被设置为1 MB，那么Kafka在收到消费者的请求后，如果有1 MB数据，就将其返回，如果没有，就在100毫秒后返回，就看哪个条件先得到满足。

### 4.5.3　fetch.max.bytes

这个属性指定了Kafka返回的数据的最大字节数（默认为50 MB）​。消费者会将服务器返回的数据放在内存中，所以这个属性被用于限制消费者用来存放数据的内存大小。broker端也有一个与之对应的配置属性，Kafka管理员可以用它来限制最大获取数量。请求的数据量越大，需要从磁盘读取的数据量就越大，通过网络发送数据的时间就越长，这可能会导致资源争用并增加broker的负载。

### 4.5.4　max.poll.records

这个属性用于控制单次调用poll()方法返回的记录条数。可以用它来控制应用程序在进行每一次轮询循环时需要处理的记录条数（不是记录的大小）​。

### 4.5.5　max.partition.fetch.bytes

这个属性指定了服务器从每个分区里返回给消费者的最大字节数（默认值是1MB）​。当KafkaConsumer.poll()方法返回ConsumerRecords时，从每个分区里返回的记录最多不超过max.partition.fetch.bytes指定的字节。需要注意的是，使用这个属性来控制消费者的内存使用量会让事情变得复杂，因为你无法控制broker返回的响应里包含多少个分区的数据。因此，对于这种情况，建议用fetch.max.bytes替代，除非有特殊的需求，比如要求从每个分区读取差不多的数据量。

### 4.5.6　session.timeout.ms和heartbeat.interval.ms

session.timeout.ms指定了消费者可以在多长时间内不与服务器发生交互而仍然被认为还“活着”​，默认是10秒。如果消费者没有在session.timeout.ms指定的时间内发送心跳给群组协调器，则会被认为已“死亡”​，协调器就会触发再均衡，把分区分配给群组里的其他消费者。session.timeout.ms与heartbeat.interval.ms紧密相关。heartbeat.interval.ms指定了消费者向协调器发送心跳的频率，session.timeout.ms指定了消费者可以多久不发送心跳。因此，我们一般会同时设置这两个属性，heartbeat.interval.ms必须比session.timeout.ms小，通常前者是后者的1/3。如果session.timeout.ms是3秒，那么heartbeat.interval.ms就应该是1秒。把session.timeout.ms设置得比默认值小，可以更快地检测到崩溃，并从崩溃中恢复，但也会导致不必要的再均衡。把session.timeout.ms设置得比默认值大，可以减少意外的再均衡，但需要更长的时间才能检测到崩溃。

### 4.5.7　max.poll.interval.ms

这个属性指定了消费者在被认为已经“死亡”之前可以在多长时间内不发起轮询。前面提到过，心跳和会话超时是Kafka检测已“死亡”的消费者并撤销其分区的主要机制。我们也提到了心跳是通过后台线程发送的，而后台线程有可能在消费者主线程发生死锁的情况下继续发送心跳，但这个消费者并没有在读取分区里的数据。要想知道消费者是否还在处理消息，最简单的方法是检查它是否还在请求数据。比如应用程序假死了，虽然还存活但是已经无法再消费消息。但是，即使设置了max.poll.records，调用poll()的时间间隔仍然很难预测。于是，设置max.poll.interval.ms就成了一种保险措施。它必须被设置得足够大，让正常的消费者尽量不触及这个阈值，但又要足够小，避免有问题的消费者给应用程序造成严重影响。这个属性的默认值为5分钟。当这个阈值被触及时，后台线程将向broker发送一个“离开群组”的请求，让broker知道这个消费者已经“死亡”​，必须进行群组再均衡，然后停止发送心跳。

### 4.5.8　default.api.timeout.ms

如果在调用消费者API时没有显式地指定超时时间，那么消费者就会在调用其他API时使用这个属性指定的值。默认值是1分钟，因为它比请求超时时间的默认值大，所以可以将重试时间包含在内。poll()方法是一个例外，因为它需要显式地指定超时时间。

### 4.5.9　request.timeout.ms

这个属性指定了消费者在收到broker响应之前可以等待的最长时间。如果broker在指定时间内没有做出响应，那么客户端就会关闭连接并尝试重连。它的默认值是30秒。不建议把它设置得比默认值小。在放弃请求之前要给broker留有足够长的时间来处理其他请求，因为向已经过载的broker发送请求几乎没有什么好处，况且断开并重连只会造成更大的开销。

### 4.5.10　auto.offset.reset

这个属性指定了消费者在读取一个没有偏移量或偏移量无效（因消费者长时间不在线，偏移量对应的记录已经过期并被删除）的分区时该做何处理。它的默认值是latest，意思是说，如果没有有效的偏移量，那么消费者将从最新的记录（在消费者启动之后写入Kafka的记录）开始读取。另一个值是earliest，意思是说，如果没有有效的偏移量，那么消费者将从起始位置开始读取记录。如果将auto.offset.reset设置为none，并试图用一个无效的偏移量来读取记录，则消费者将抛出异常。

### 4.5.11　enable.auto.commit

这个属性指定了消费者是否自动提交偏移量，默认值是true。你可以把它设置为false，选择自己控制何时提交偏移量，以尽量避免出现数据重复和丢失。如果它被设置为true，那么还有另外一个属性auto.commit.interval.ms可以用来控制偏移量的提交频率。

### 4.5.12　partition.assignment.strategy

我们知道，分区会被分配给群组里的消费者。PartitionAssignor根据给定的消费者和它们订阅的主题来决定哪些分区应该被分配给哪个消费者。Kafka提供了几种默认的分配策略。

__区间(range)__

这个策略会把每一个主题的若干个连续分区分配给消费者。假设消费者C1和消费者C2同时订阅了主题T1和主题T2，并且每个主题有3个分区。那么消费者C1有可能会被分配到这两个主题的分区0和分区1，消费者C2则会被分配到这两个主题的分区2。因为每个主题拥有奇数个分区，并且都遵循一样的分配策略，所以第一个消费者会分配到比第二个消费者更多的分区。只要使用了这个策略，并且分区数量无法被消费者数量整除，就会出现这种情况。

__轮询(roundRobin)__

这个策略会把所有被订阅的主题的所有分区按顺序逐个分配给消费者。如果使用轮询策略为消费者C1和消费者C2分配分区，那么消费者C1将分配到主题T1的分区0和分区2以及主题T2的分区1，消费者C2将分配到主题T1的分区1以及主题T2的分区0和分区2。一般来说，如果所有消费者都订阅了相同的主题（这种情况很常见）​，那么轮询策略会给所有消费者都分配相同数量（或最多就差一个）的分区。

__黏性(sticky)__

设计黏性分区分配器的目的有两个：一是尽可能均衡地分配分区，二是在进行再均衡时尽可能多地保留原先的分区所有权关系，减少将分区从一个消费者转移给另一个消费者所带来的开销。如果所有消费者都订阅了相同的主题，那么黏性分配器初始的分配比例将与轮询分配器一样均衡。后续的重新分配将同样保持均衡，但减少了需要移动的分区的数量。如果同一个群组里的消费者订阅了不同的主题，那么黏性分配器的分配比例将比轮询分配器更加均衡。

__协作黏性(cooperative sticky)__

这个分配策略与黏性分配器一样，只是它支持协作（增量式）再均衡，在进行再均衡时消费者可以继续从没有被重新分配的分区读取消息

可以通过partition.assignment.strategy来配置分区策略，默认值是org.apache.kafka.clients.consumer.RangeAssignor，它实现了区间策略。你也可以把它改成org.apache.kafka.clients.consumer.RoundRobinAssignor、org.apache.kafka.clients.consumer.StickyAssignor或org.apache.kafka.clients.consumer.CooperativeStickyAssignor。还可以使用自定义分配策略，如果是这样，则需要把partition.assignment.strategy设置成自定义类的名字。

### 4.5.13　client.id

这个属性可以是任意字符串，broker用它来标识从客户端发送过来的请求，比如获取请求。它通常被用在日志、指标和配额中。

### 4.5.14　client.rack

在默认情况下，消费者会从每个分区的首领副本那里获取消息。但是，如果集群跨越了多个数据中心或多个云区域，那么让消费者从位于同一区域的副本那里获取消息就会具有性能和成本方面的优势。要从最近的副本获取消息，需要设置client.rack这个参数，用于标识客户端所在的区域。然后，可以将broker的replica.selector.class参数值改为org.apache.kafka.common.replica.RackAwareReplicaSelector。你也可以实现自己的replica.selector.class，根据客户端元数据和分区元数据选择想要读取的副本。

### 4.5.15　group.instance.id

这个属性可以是任意具有唯一性的字符串，被用于消费者群组的固定名称。

### 4.5.16　receive.buffer.bytes和send.buffer.bytes

这两个属性分别指定了socket在读写数据时用到的TCP缓冲区大小。如果它们被设置为–1，就使用操作系统的默认值。如果生产者或消费者与broker位于不同的数据中心，则可以适当加大它们的值，因为跨数据中心网络的延迟一般都比较高，而带宽又比较低。

### 4.5.17　offsets.retention.minutes

这是broker端的一个配置属性，需要注意的是，它也会影响消费者的行为。只要消费者群组里有活跃的成员（也就是说，有成员通过发送心跳来保持其身份）​，群组提交的每一个分区的最后一个偏移量就会被Kafka保留下来，在进行重分配或重启之后就可以获取到这些偏移量。但是，如果一个消费者群组失去了所有成员，则Kafka只会按照这个属性指定的时间（默认为7天）保留偏移量。一旦偏移量被删除，即使消费者群组又“活”了过来，它也会像一个全新的群组一样，没有了过去的消费记忆

## 4.6　提交和偏移量

每次调用poll()方法，它总是会返回还没有被消费者读取过的记录，这意味着我们有办法来追踪哪些记录是被群组里的哪个消费者读取过的。之前提到过，Kafka不像其他JMS队列系统那样需要收到来自消费者的确认，这是Kafka的一个独特之处。相反，消费者可以用Kafka来追踪已读取的消息在分区中的位置（偏移量）​。

我们把更新分区当前读取位置的操作叫作偏移量提交。与传统的消息队列不同，Kafka不会提交每一条记录。相反，消费者会将已成功处理的最后一条消息提交给Kafka，并假定该消息之前的每一条消息都已成功处理。

那么消费者是如何提交偏移量的呢？消费者会向一个叫作 __consumer_offset的主题发送消息，消息里包含每个分区的偏移量。如果消费者一直处于运行状态，那么偏移量就没有什么实际作用。但是，如果消费者发生崩溃或有新的消费者加入群组，则会触发再均衡。再均衡完成之后，每个消费者可能会被分配新的分区，而不是之前读取的那个。为了能够继续之前的工作，消费者需要读取每个分区最后一次提交的偏移量，然后从偏移量指定的位置继续读取消息。

如果最后一次提交的偏移量小于客户端处理的最后一条消息的偏移量，那么处于两个偏移量之间的消息就会被重复处理。

![avatar](img/11.png)

如果最后一次提交的偏移量大于客户端处理的最后一条消息的偏移量，那么处于两个偏移量之间的消息就会丢失。

![avatar](img/12.png)

如果使用自动提交或不指定提交的偏移量，那么将默认提交poll()返回的最后一个位置之后的偏移量。

### 4.6.1　自动提交

最简单的提交方式是让消费者自动提交偏移量。如果enable.auto.commit被设置为true，那么每过5秒，消费者就会自动提交poll()返回的最大偏移量。提交时间间隔通过auto.commit.interval.ms来设定，默认是5秒。与消费者中的其他处理过程一样，自动提交也是在轮询循环中进行的。消费者会在每次轮询时检查是否该提交偏移量了，如果是，就会提交最后一次轮询返回的偏移量。

假设我们使用默认的5秒提交时间间隔，并且消费者在最后一次提交偏移量之后3秒会发生崩溃。再均衡完成之后，接管分区的消费者将从最后一次提交的偏移量的位置开始读取消息。这个偏移量实际上落后了3秒，所以在这3秒内到达的消息会被重复处理。

在使用自动提交时，到了该提交偏移量的时候，轮询方法将提交上一次轮询返回的偏移量，但它并不知道具体哪些消息已经被处理过了，所以，在再次调用poll()之前，要确保上一次poll()返回的所有消息都已经处理完毕（调用close()方法也会自动提交偏移量）​。通常情况下这不会有什么问题，但在处理异常或提前退出轮询循环时需要特别小心。虽然自动提交很方便，但是没有为避免开发者重复处理消息留有余地。

### 4.6.2　提交当前偏移量

消费者API提供了另一种提交偏移量的方式，开发者可以在必要的时候手动提交当前偏移量，而不是基于时间间隔提交。把enable.auto.commit设置为false，让应用程序自己决定何时提交偏移量。使用commitSync()提交偏移量是最简单可靠的方式。这个API会提交poll()返回的最新偏移量，提交成功后马上返回，如果由于某些原因提交失败就抛出异常。

需要注意的是，commitSync()将会提交poll()返回的最新偏移量，所以，如果你在处理完所有记录之前就调用了commitSync()，那么一旦应用程序发生崩溃，就会有丢失消息的风险（消息已被提交但未被处理）​。

```java
Duration timeout = Duration.ofMillis(100);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(timeout);
    for (ConsumerRecord<String, String> record : records) {
        System.out.printf("topic = %s, partition = %d, offset =
            %d, customer = %s, country = %s\n",
            record.topic(), record.partition(),
            record.offset(), record.key(), record.value()); ➊
    }
    try {
        consumer.commitSync(); ➋
    } catch (CommitFailedException e) {
        log.error("commit failed", e) ➌
    }
}
```
### 4.6.3　异步提交

手动提交有一个缺点，在broker对请求做出回应之前，应用程序会一直阻塞，这样会限制应用程序的吞吐量。可以通过降低提交频率来提升吞吐量，但如果发生了再均衡，则会增加潜在的消息重复。这个时候可以使用异步提交API。只管发送请求，无须等待broker做出响应。

```java
Duration timeout = Duration.ofMillis(100);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(timeout);
    for (ConsumerRecord<String, String> record : records) {
        System.out.printf("topic = %s, partition = %s,
            offset = %d, customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(),
            record.key(), record.value());
    }
    consumer.commitAsync(); ➊
}
```
在提交成功或碰到无法恢复的错误之前，commitSync()会一直重试，但commitAsync()不会，这是commitAsync()的一个缺点。之所以不进行重试，是因为commitAsync()在收到服务器端的响应时，可能已经有一个更大的偏移量提交成功。假设我们发出一个提交偏移量2000的请求，这个时候出现了短暂的通信问题，服务器收不到请求，自然也不会做出响应。与此同时，我们处理了另外一批消息，并成功提交了偏移量3000。如果此时commitAsync()重新尝试提交偏移量2000，则有可能在偏移量3000之后提交成功。这个时候如果发生再均衡，就会导致消息重复。

commitAsync()也支持回调，回调会在broker返回响应时执行。回调经常被用于记录偏移量提交错误或生成指标，如果要用它来重试提交偏移量，那么一定要注意提交顺序。

```java
Duration timeout = Duration.ofMillis(100);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(timeout);
    for (ConsumerRecord<String, String> record : records) {
        System.out.printf("topic = %s, partition = %s,
        offset = %d, customer = %s, country = %s\n",
        record.topic(), record.partition(), record.offset(),
        record.key(), record.value());
    }
    consumer.commitAsync(new OffsetCommitCallback() {
        public void onComplete(Map<TopicPartition,
        OffsetAndMetadata> offsets, Exception e) {
            if (e != null)
                log.error("Commit failed for offsets {}", offsets, e);
        }
    }); ➊
}
```

可以用一个单调递增的消费者序列号变量来维护异步提交的顺序。每次调用commitAsync()后增加序列号，并在回调中更新序列号变量。在准备好进行重试时，先检查回调的序列号与序列号变量是否相等。如果相等，就说明没有新的提交，可以安全地进行重试。如果序列号变量比较大，则说明已经有新的提交了，此时应该停止重试。

### 4.6.4　同步和异步组合提交

如果是消费者被关闭，那么一般会使用commitAsync()和commitSync()的组合。这种模式的原理如下。

```java
Duration timeout = Duration.ofMillis(100);

try {
    while (!closing) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("topic = %s, partition = %s, offset = %d,
                customer = %s, country = %s\n",
                record.topic(), record.partition(),
                record.offset(), record.key(), record.value());
        }
        consumer.commitAsync();➊
    }
    consumer.commitSync();➋
} catch (Exception e) {
    log.error("Unexpected error", e);
} finally {
        consumer.close();
}
```
❶ 如果一切正常，就用commitAsync()提交偏移量。这样速度更快，而且即使这次提交失败，下一次提交也会成功。❷ 如果直接关闭消费者，那么就没有所谓的“下一次提交”了。commitSync()会一直重试，直到提交成功或发生无法恢复的错误。

### 4.6.5　提交特定的偏移量

提交最后一个偏移量的频率应该与处理消息批次的频率一样。但如果想要更频繁地提交偏移量该怎么办？如果poll()返回了一大批数据，那么为了避免可能因再均衡引起的消息重复，想要在批次处理过程中提交偏移量该怎么办？

消费者API允许在调用commitSync()和commitAsync()时传给它们想要提交的分区和偏移量。假设你正在处理一个消息批次，刚处理好来自主题“customers”的分区3的消息，它的偏移量是5000，那么就可以调用commitSync()来提交这个分区的偏移量5001。需要注意的是，因为一个消费者可能不止读取一个分区，你需要跟踪所有分区的偏移量，所以通过这种方式提交偏移量会让代码变得复杂。

```java
private Map<TopicPartition, OffsetAndMetadata> currentOffsets =
    new HashMap<>(); ➊
int count = 0;

....
Duration timeout = Duration.ofMillis(100);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(timeout);
    for (ConsumerRecord<String, String> record : records) {
        System.out.printf("topic = %s, partition = %s, offset = %d,
            customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(),
            record.key(), record.value()); ➋
        currentOffsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset()+1, "no metadata")); ➌
        if (count % 1000 == 0) ➍
            consumer.commitAsync(currentOffsets, null); ➎
        count++;
    }
}
```

## 4.7　再均衡监听器

让你可以在消费者分配到新分区或旧分区被移除时执行一些代码逻辑。你所要做的就是在调用subscribe()方法时传进去一个ConsumerRebalanceListener对象。ConsumerRebalanceListener有3个需要实现的方法。

public void onPartitionsAssigned(Collection<TopicPartition> partitions)这个方法会在重新分配分区之后以及消费者开始读取消息之前被调用。你可以在这个方法中准备或加载与分区相关的状态信息、找到正确的偏移量，等等。这里所有的事情都应该保证在max.poll.timeout.ms内完成，以便消费者可以成功地加入群组。

public void onPartitionsRevoked(Collection<TopicPartition> partitions)这个方法会在消费者放弃对分区的所有权时调用——可能是因为发生了再均衡或者消费者正在被关闭。通常情况下，如果使用了主动再平衡算法，那么这个方法会在再平衡开始之前以及消费者停止读取消息之后调用。如果使用了协作再平衡算法，那么这个方法会在再均衡结束时调用，而且只涉及消费者放弃所有权的那些分区。如果你要提交偏移量，那么可以在这里提交，无论是哪个消费者接管这个分区，它都知道应该从哪里开始读取消息。

public void onPartitionsLost(Collection<TopicPartition> partitions)这个方法只会在使用了协作再均衡算法并且之前不是通过再均衡获得的分区被重新分配给其他消费者时调用（之前通过再均衡获得的分区被重新分配时会调用onPartitions Revoked()）​。你可以在这里清除与这些分区相关的状态或资源。需要注意的是，在清理状态时要非常小心，因为分区的新所有者可能也保存了分区状态，需要避免发生冲突。如果你没有实现这个方法，则onPartitionsRevoked()将被调用。

```java
private Map<TopicPartition, OffsetAndMetadata> currentOffsets =
    new HashMap<>();
Duration timeout = Duration.ofMillis(100);

private class HandleRebalance implements ConsumerRebalanceListener { ➊
    public void onPartitionsAssigned(Collection<TopicPartition>
        partitions) { ➋
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Lost partitions in rebalance. " +
            "Committing current offsets:" + currentOffsets);
        consumer.commitSync(currentOffsets); ➌
    }
}

try {
    consumer.subscribe(topics, new HandleRebalance()); ➍

    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("topic = %s, partition = %s, offset = %d,
                customer = %s, country = %s\n",
                record.topic(), record.partition(), record.offset(),
                record.key(), record.value());
            currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset()+1, null));
        }
        consumer.commitAsync(currentOffsets, null);
    }
} catch (WakeupException e) {
    // 忽略异常
} catch (Exception e) {
    log.error("Unexpected error", e);
} finally {
    try {
        consumer.commitSync(currentOffsets);
    } finally {
        consumer.close();
        System.out.println("Closed consumer and we are done");
    }
}
```

## 4.8　从特定偏移量位置读取记录

如果你想从分区的起始位置读取所有的消息，或者直接跳到分区的末尾读取新消息，那么Kafka API分别提供了两个方法：seekToBeginning(Collection<TopicPartition> tp) 和seekToEnd(Collection<TopicPartition> tp)。

Kafka还提供了用于查找特定偏移量的API。这个API有很多用途，比如，对时间敏感的应用程序在处理速度滞后的情况下可以向前跳过几条消息，或者如果消费者写入的文件丢失了，则它可以重置偏移量，回到某个位置进行数据恢复。

```java
Long oneHourEarlier = Instant.now().atZone(ZoneId.systemDefault())
          .minusHours(1).toEpochSecond();
Map<TopicPartition, Long> partitionTimestampMap = consumer.assignment()
        .stream()
        .collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier)); ➊
Map<TopicPartition, OffsetAndTimestamp> offsetMap
        = consumer.offsetsForTimes(partitionTimestampMap); ➋

for(Map.Entry<TopicPartition,OffsetAndTimestamp> entry: offsetMap.entrySet()) {
    consumer.seek(entry.getKey(), entry.getValue().offset()); ➌
}
```
## 4.9　如何退出

如果你确定马上要关闭消费者（即使消费者还在等待一个poll()返回）​，那么可以在另一个线程中调用consumer.wakeup()。如果轮询循环运行在主线程中，那么可以在ShutdownHook里调用这个方法。需要注意的是，consumer.wakeup()是消费者唯一一个可以在其他线程中安全调用的方法。调用consumer.wakeup()会导致poll()抛出WakeupException，如果调用consumer.wakeup()时线程没有在轮询，那么异常将在下一次调用poll()时抛出。不一定要处理WakeupException，但在退出线程之前必须调用consumer.close()。消费者在被关闭时会提交还没有提交的偏移量，并向消费者协调器发送消息，告知自己正在离开群组。协调器会立即触发再均衡，被关闭的消费者所拥有的分区将被重新分配给群组里其他的消费者，不需要等待会话超时。

```java
Runtime.getRuntime().addShutdownHook(new Thread() {
    public void run() {
        System.out.println("Starting exit...");
        consumer.wakeup(); ➊
        try {
            mainThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
});

...
Duration timeout = Duration.ofMillis(10000); ➋

try {
    // 一直循环，直到按下Ctrl-C组合键，关闭钩子会在退出时做清理工作
    while (true) {
        ConsumerRecords<String, String> records =
            movingAvg.consumer.poll(timeout);
        System.out.println(System.currentTimeMillis() +
            "-- waiting for data...");
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s\n",
                record.offset(), record.key(), record.value());
        }
        for (TopicPartition tp: consumer.assignment())
            System.out.println("Committing offset at position:" +
                consumer.position(tp));
        movingAvg.consumer.commitSync();
    }
} catch (WakeupException e) {
    // 忽略异常 ➌
} finally {
    consumer.close(); ➍
    System.out.println("Closed consumer and we are done");
}
```

❶ ShutdownHook运行在单独的线程中，所以退出轮询循环最安全的方式只能是调用wakeup()。❷ 一个比较长的轮询超时时间。如果轮询的时间足够短，并且你不介意在退出之前等一小会儿，那么就没有必要调用wakeup()，只需在每次轮询时检查一下原子布尔变量即可。较长的轮询超时时间在处理低吞吐量主题时比较有用，因为当broker没有新数据返回时，客户端在轮询时占用的CPU时间会更少。❸ 因为在另一个线程中调用了wakeup()，所以poll()会抛出WakeupException。你可能想捕获异常，确保应用程序不会意外退出，但实际上在这里无须对它做任何处理。❹ 在退出之前，确保彻底关闭了消费者。

## 4.10　反序列化器

很显然，生成消息所使用的序列化器与读取消息所使用的反序列化器应该是相对应的。如果用IntSerializer序列化对象，但用StringDeserializer反序列化它，那么就不会出现什么好结果。对开发者来说，他们必须知道写入主题的消息使用的是哪一种序列化器，并确保每个主题里只包含能够被反序列化器解析的数据。

## 4.11　独立的消费者：为什么以及怎样使用不属于任何群组的消费者

如果知道需要读取哪些分区，就不需要订阅主题了，可以直接将目标分区分配给消费者。消费者既可以订阅主题（并加入消费者群组）​，也可以为自己分配分区，但不能同时做这两件事情。

```java
Duration timeout = Duration.ofMillis(100);
List<PartitionInfo> partitionInfos = null;
partitionInfos = consumer.partitionsFor("topic"); ➊

if (partitionInfos != null) {
    for (PartitionInfo partition : partitionInfos)
        partitions.add(new TopicPartition(partition.topic(),
            partition.partition()));
    consumer.assign(partitions); ➋

    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);

        for (ConsumerRecord<String, String> record: records) {
            System.out.printf("topic = %s, partition = %s, offset = %d,
                customer = %s, country = %s\n",
                record.topic(), record.partition(), record.offset(),
                record.key(), record.value());
        }
        consumer.commitSync();
    }
}
```
# 第5章　编程式管理Kafka

Kafka在0.11版本中加入了AdminClient，为之前只能通过命令行完成的管理功能提供了编程API：查看、创建和删除主题，描述集群，管理ACL和修改配置。

我们将重点关注最常用的功能：主题管理、消费者群组管理和配置管理。

## 5.1　AdminClient概览

### 5.1.1　异步和最终一致性API

AdminClient是异步的，它的每一个方法在向集群控制器发出请求后会立即返回一个或多个Future对象。Future对象是异步操作的结果，提供了用于检查异步操作状态、取消操作、等待操作完成以及在操作完成后执行其他函数的一些方法。AdminClient会将Future对象封装到Result对象中，Result对象提供了一些方法以用于等待操作完成和执行常见的后续操作。例如，KafkaAdminClient.createTopics会返回CreateTopicsResult对象，你可以用它来等待所有主题创建完成、检查每个主题的状态，并在主题创建成功之后获取主题的配置信息。

从控制器到broker的元数据传播是异步的，所以当控制器状态被完全更新时，AdminClient API返回的Future将被视为已完成。这个时候，并不是每个broker都知道发生了状态变更，所以listTopics请求可能会由不包含最新创建主题的broker负责处理。这叫作最终一致性：最终每个broker都会知道每一个主题的存在，但我们不能保证是在什么时候。

### 5.1.2　配置参数

AdminClient的每一个方法都会接受一个特定于该方法的Options对象作为参数。例如，listTopics方法的参数是ListTopicsOptions对象，describeCluster的参数是DescribeClusterOptions对象。这些对象包含了broker将如何处理请求的配置参数。所有AdminClient方法都会有的一个配置参数是timeoutMs：它指定了客户端在抛出TimeoutException之前等待集群返回响应的时间。这个参数用于限制应用程序被AdminClient操作阻塞的时间。其他参数还包括listTopics是否应该返回集群的内部主题，以及describeCluster是否应该返回客户端被授权了哪些集群操作。

### 5.1.3　扁平的结构

Kafka协议支持的所有管理操作都可以直接用KafkaAdminClient来实现。AdminClient的接口没有对象层次结构或命名空间，这有点儿争议，因为这个接口有很多方法，但这么做最主要的好处是，如果你想知道如何通过编程的方式执行Kafka管理操作，那么只需要搜索一个JavaDoc，无须担心忘记搜索哪里了，而且IDE可以自动完成代码，非常方便。如果你要找的东西不在AdminClient中，则说明它还没有被实现（但欢迎贡献者参与贡献）​。

## 5.2　AdminClient生命周期：创建、配置和关闭

要使用Kafka的AdminClient，首先需要构造一个AdminClient对象。

```java
Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
AdminClient admin = AdminClient.create(props);
// TODO: 用AdminClient做一些有用的事情
admin.close(Duration.ofSeconds(30));
```
create静态方法会接受一个包含配置参数的Properties对象作为参数。唯一必须提供的配置参数是集群的URI：用逗号分隔的要连接的broker地址清单。通常，在生产环境中至少需要指定3个broker地址，以防其中一个不可用。

### 5.2.1　client.dns.lookup

在默认情况下，Kafka根据连接串中提供的主机名来验证、解析和创建连接（然后是根据advertised.listeners指定的监听器监听的主机名）​。这种方式在大多数情况下没有问题，但不适用于两个重要的场景：使用DNS别名以及单个DNS映射了多个IP地址。二者看起来差不多，但还是略有不同。下面让我们更详细地了解一下这两个互斥场景。

__1.使用DNS别名__

假设你有多个broker，它们的命名规则是这样的：broker1.hostname.com、broker2.hostname.com，以此类推。与其在连接串中指定所有这些地址（维护起来比较困难）​，不如创建一个DNS别名，将其与所有的主机地址映射起来。可以用all-brokers.hostname.com作为连接串，因为你并不关心哪个broker将接受来自客户端的初始连接。这样做非常方便，除非你启用了SASL身份验证。如果使用了SASL，那么客户端会尝试对all-brokers.hostname.com进行身份验证，而实际的服务器主体可能是broker2.hostname.com。如果名字不匹配，则SASL将拒绝身份验证（broker认证有可能是中间人攻击）​，进而连接失败。对于这个场景，需要配置client.dns.lookup=resolve_canonical_bootstrap_servers_only，客户端将会“展开”DNS别名，其效果与在连接串中指定所有的broker主机地址一样。

__2.单个DNS映射了多个IP地址__

在现代网络架构中，通常会将所有的broker放在代理或负载均衡器后面。这在Kubernetes集群中尤为常见，因为Kubernetes集群需要用负载均衡器来转发外部连接。在这些场景中，我们不希望负载均衡器成为单点故障点。因此，常见的做法是将broker1.hostname.com指向一组IP地址，也就是负载均衡器的IP地址，它们总是能够将流量路由到相同的broker上。这些IP地址可能会发生变化。在默认情况 下，Kafka客户端会尝试连接第一个IP地址。如果这个IP地址不可用，那么客户端就无法连接到broker，即使broker是可用的。因此，建议配置client.dns.lookup=use_all_dns_ips，确保客户端可以充分利用高可用负载均衡器所带来的好处。

### 5.2.2　request.timeout.ms

这个参数指定了应用程序等待AdminClient返回响应的时间，包括客户端收到可重试错误时进行重试的时间。这个参数的默认值是120秒，时间有点儿长，但确实有一些AdminClient操作，特别是消费者群组操作，需要较长的时间。正如5.1节中提到的，每个AdminClient方法都会接受一个Options对象作为参数，这个对象可以包含超时时间。如果AdminClient操作位于应用程序的关键路径上，你可能就会想要设置短一点儿的超时时间，并通过其他方式处理Kafka未能及时做出响应的问题。一个常见的例子是，应用程序会在第一次启动时尝试验证主题是否存在，如果Kafka在30秒内未能做出响应，你可能就会选择继续启动应用程序，稍后再来验证主题是否存在（或者完全跳过验证）​。

## 5.3　基本的主题管理操作

先来列出集群的所有主题。

```java
ListTopicsResult topics = admin.listTopics();
topics.names().get().forEach(System.out::println);
```
需要注意的是，admin.listTopics()返回的ListTopicsResult是对一组Future对象的包装器。topics.name()返回的是一组主题名字的Future对象。当调用Future的get()方法时，执行线程将会等待，直到服务器返回一组主题名字或抛出超时异常。

现在，尝试做一些更有用的事情：检查主题是否存在，如果不存在，就创建一个。要检查主题是否存在，一种方法是获取所有主题的名字，然后检查你需要的主题是否在列表中。在大型集群中，这么做可能效率低下，况且有时候你可能不只想检查主题是否存在，还想知道主题分区和副本的数量是否正确。例如，Connect和Conﬂuent模式注册表就使用一个主题来保存配置信息，在启动时，它们会检查这个主题是否存在、主题是否只有一个分区（确保配置变更严格有序）​、是否有3个副本（确保可用性）​，以及是否被压实（及时更新旧的配置信息）​。

```java
DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST); ➊

try {
    topicDescription = demoTopic.values().get(TOPIC_NAME).get(); ➋
    System.out.println("Description of demo topic:" + topicDescription);

    if (topicDescription.partitions().size() != NUM_PARTITIONS) { ➌
      System.out.println("Topic has wrong number of partitions. Exiting.");
      System.exit(-1);
    }
} catch (ExecutionException e) { ➍
    // 对于大部分异常，提前退出
    if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
        e.printStackTrace();
        throw e;
    }

    // 如果执行到这里，则说明主题不存在
    System.out.println("Topic " + TOPIC_NAME +
        " does not exist. Going to create it now");
    // 需要注意的是，分区和副本数是可选的
    // 如果没有指定，那么将使用broker的默认配置
    CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(
            new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR))); ➎

    // 检查主题是否已创建成功：
    if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) { ➏
        System.out.println("Topic has wrong number of partitions.");
        System.exit(-1);
    }
}
```

现在我们有了一个主题，接下来把它删除。

```java
admin.deleteTopics(TOPIC_LIST).all().get();

// 检查主题是否已删除
// 需要注意的是，由于删除是异步操作，这个时候主题可能还存在
try {
    topicDescription = demoTopic.values().get(TOPIC_NAME).get();
    System.out.println("Topic " + TOPIC_NAME + " is still around");
} catch (ExecutionException e) {
    System.out.println("Topic " + TOPIC_NAME + " is gone");
}
```
到目前为止，所有的示例都调用了Future对象的get()方法。大多数时候，这些已经足够了，毕竟管理操作不会太多，而且等待操作完成或超时都是可接受的。但有一种情况例外：你正在向一个需要处理大量管理请求的服务器写入数据。在这种情况下，你并不希望在等待Kafka返回响应的同时阻塞服务器线程，而是希望服务器继续接受来自客户端的请求，然后将请求发送给Kafka，等Kafka返回响应后再将响应发送给客户端。这个时候，KafkaFuture会非常有用。下面是一个简单的例子。

```java
vertx.createHttpServer().requestHandler(request -> { ➊
    String topic = request.getParam("topic"); ➋
    String timeout = request.getParam("timeout");
    int timeoutMs = NumberUtils.toInt(timeout, 1000);

    DescribeTopicsResult demoTopic = admin.describeTopics( ➌
            Collections.singletonList(topic),
            new DescribeTopicsOptions().timeoutMs(timeoutMs));

    demoTopic.values().get(topic).whenComplete( ➍
            new KafkaFuture.BiConsumer<TopicDescription, Throwable>() {
               @Override
               public void accept(final TopicDescription topicDescription,
                                  final Throwable throwable) {
                   if (throwable != null) {
                      request.response().end("Error trying to describe topic "
                              + topic + " due to " + throwable.getMessage()); ➎
                   } else {
                       request.response().end(topicDescription.toString()); ➏
                   }
                }
            });
}).listen(8080);
```

## 5.4　配置管理

配置管理是通过描述和更新一系列配资资源(ConfigResource)来实现的。配置资源可以是broker、broker日志记录器和主题。我们通常会用kafka-config.sh或其他Kafka管理工具来检查和修改broker及broker日志配置，但主题配置管理是在应用程序中完成的。

例如，很多应用程序使用了压实的主题，它们会定期（为安全起见，要比默认的保留期限更加频繁一些）检查主题是否被压实，如果没有，就采取相应的行动来纠正主题配置。

```java
ConfigResource configResource =
        new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME); ➊
DescribeConfigsResult configsResult =
        admin.describeConfigs(Collections.singleton(configResource));
Config configs = configsResult.all().get().get(configResource);
// 打印非默认配置
configs.entries().stream().filter(
        entry -> !entry.isDefault()).forEach(System.out::println); ➋

// 检查主题是否被压实
ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
        TopicConfig.CLEANUP_POLICY_COMPACT);
if (!configs.entries().contains(compaction)) {
    // 如果主题没有被压实，就将其压实
    Collection<AlterConfigOp> configOp = new ArrayList<AlterConfigOp>();
    configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET)); ➌
    Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
    alterConf.put(configResource, configOp);
    admin.incrementalAlterConfigs(alterConf).all().get();
} else {
    System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
}
```
## 5.5　消费者群组管理

但是，在某些情况下，即使应用程序没有预先内置这个功能，你也希望它们能够重新处理消息。一种情况是在发生事故期间对应用程序进行诊断，另一种情况是在故障转移过程中准备在新集群中运行应用程序.

### 5.5.1　查看消费者群组

```java
admin.listConsumerGroups().valid().get().forEach(System.out::println);
```

如果想了解更多有关某些群组的信息，那么可以用如下信息描述它们。

```java
ConsumerGroupDescription groupDescription = admin
        .describeConsumerGroups(CONSUMER_GRP_LIST)
        .describedGroups().get(CONSUMER_GROUP).get();
        System.out.println("Description of group " + CONSUMER_GROUP
                + ":" + groupDescription);
```

但这里缺了一个比较重要的信息——我们想要知道消费者群组最后消费的每个分区的偏移量以及相比最新消息滞后了多少。在过去，获取这些信息唯一的方法是解析消费者群组写到内部主题的偏移量消息。虽然这种方法可以实现我们的目的，但Kafka不保证内部消息格式是兼容的，因此不建议使用。下面来看看AdminClient是如何获取这些信息的。

```java
Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get(); ➊

Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();

for(TopicPartition tp: offsets.keySet()) {
    requestLatestOffsets.put(tp, OffsetSpec.latest()); ➋
}

Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
        admin.listOffsets(requestLatestOffsets).all().get();

for (Map.Entry<TopicPartition, OffsetAndMetadata> e: offsets.entrySet()) { ➌
    String topic = e.getKey().topic();
    int partition =  e.getKey().partition();
    long committedOffset = e.getValue().offset();
    long latestOffset = latestOffsets.get(e.getKey()).offset();

    System.out.println("Consumer group " + CONSUMER_GROUP
            + " has committed offset " + committedOffset
            + " to topic " + topic + " partition " + partition
            + ". The latest offset in the partition is "
            + latestOffset + " so consumer group is "
            + (latestOffset - committedOffset) + " records behind");
}
```
❶ 获取消费者群组读取的所有主题和分区，以及每个分区最新的提交偏移量。与describeConsumerGroups不同，listConsumerGroupOffsets只接受一个消费者群组而不是一个集合作为参数。❷ 我们希望获取到结果集中每一个分区最后一条消息的偏移量。OffsetSpec提供了3个非常方便的实现：earliest()、latest()和forTimestamp()，分别用于获取分区中最早和最近的偏移量，以及在指定时间或紧接在指定时间之后写入的消息的偏移量。❸ 最后，遍历所有分区，将最近提交的偏移量、分区中最近的偏移量以及它们之间的差值打印出来。

### 5.5.2　修改消费者群组

到目前为止，我们都只是查看消费者群组的信息。AdminClient也提供了修改消费者群组的方法：删除群组、移除成员、删除提交的偏移量和修改偏移量。SRE通常用它们来构建临时工具，以在紧急情况下进行故障恢复。

在所有这些方法当中，修改偏移量是最有用的。删除偏移量似乎就是让消费者“从头开始”读取数据，但实际上也取决于消费者的配置——如果消费者在启动时没有可用的偏移量，那么是从头开始读取还是直接跳到最新的位置开始读取呢？除非配置了auto.offset.reset，否则就无从得知。显式地将提交的偏移量修改为最早的偏移量，可以强制消费者从主题开头位置开始读取，实际上就是“重置”消费者。

```java
Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
    admin.listOffsets(requestEarliestOffsets).all().get(); ➊

Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e:
        earliestOffsets.entrySet()) {
  resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset())); ➋
}

try {
  admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get(); ➌
} catch (ExecutionException e) {
  System.out.println("Failed to update the offsets committed by group "
            + CONSUMER_GROUP + " with error " + e.getMessage());
  if (e.getCause() instanceof UnknownMemberIdException)
      System.out.println("Check if consumer group is still active."); ➍
}
```

## 5.7　高级的管理操作

### 5.7.1　为主题添加分区

我们很少需要为主题添加分区，而且添加分区可能存在风险。你需要确保这个操作不会影响正在消费该主题的应用程序。但是，有时候确实会遇到已有分区的吞吐量达到上限的情况，这个时候则别无选择，只能添加分区。可以用createPartitions方法为主题添加分区。需要注意的是，如果一次性为多个主题添加分区，则可能会出现一些主题添加成功一些主题添加失败的情况。

```java
Map<String, NewPartitions> newPartitions = new HashMap<>();
newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(NUM_PARTITIONS+2)); ➊
admin.createPartitions(newPartitions).all().get();
```

❶ 在添加分区时，需要指定添加分区后主题将拥有的分区总数，而不是要添加的新分区的数量。


### 5.7.2　从主题中删除消息

deleteRecords方法会将所有偏移量早于指定偏移量的消息标记为已删除，使消费者无法读取这些数据。这个方法将返回被删除的消息的最大偏移量，这样我们就可以检查删除操作是否按预期执行了。从磁盘上彻底删除数据是异步进行的。需要注意的是，可以用listOffsets方法获取在特定时间点或之后写入的消息的偏移量。也可以组合使用这些方法来删除早于任意特定时间点的消息。

```java
Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> olderOffsets =
        admin.listOffsets(requestOlderOffsets).all().get();
Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>  e:
        olderOffsets.entrySet())
    recordsToDelete.put(e.getKey(),
            RecordsToDelete.beforeOffset(e.getValue().offset()));
admin.deleteRecords(recordsToDelete).all().get();
```

### 5.7.3　首领选举

__首选首领选举__

每一个分区都有一个可以被指定为首选首领的副本。如果所有分区的首领都是它们的首选首领副本，那么每个broker上的首领数量应该是均衡的。在默认情况下，Kafka每5分钟会检查一次首领是否就是首选首领副本，如果不是，但它有资格成为首领，就会选择首选首领副本作为首领。如果auto.leader.rebalance.enable被设置为false，或者你想快一点儿执行选举，则可以调用electLeader()方法。

__不彻底的首领选举__

如果一个分区的首领副本变得不可用，而其他副本没有资格成为首领（通常是因为缺少数据）​，那么这个分区将没有首领，也就不可用了。解决这个问题的一种方法是触发不彻底的首领选举，也就是选举一个本来没有资格成为首领的副本作为首领。这可能导致数据丢失——所有写入旧首领但未被复制到新首领的消息都将丢失。electLeader()方法也可以用来触发不彻底的首领选举。这个方法是异步的，也就是说，即使它成功返回，也需要一段时间才能让所有broker都知道发生了首领选举，在这期间调用describeTopics()可能会返回不一致的结果。如果你触发了多个分区的首领选举，则可能会导致一些分区执行成功，一些分区执行失败。

```java
Set<TopicPartition> electableTopics = new HashSet<>();
electableTopics.add(new TopicPartition(TOPIC_NAME, 0));
try {
    admin.electLeaders(ElectionType.PREFERRED, electableTopics).all().get(); ➊
} catch (ExecutionException e) {
    if (e.getCause() instanceof ElectionNotNeededException) {
        System.out.println("All leaders are preferred already"); ➋
    }
}
```

### 5.7.4　重新分配副本

有时候，你可能不喜欢某些副本所在的位置。例如，一个broker超载了，你可能想移走一些副本；或者你可能想增加更多的副本；或者你可能想将一个broker所有的副本都移走，以便关闭整台机器；又或者可能有一些主题太过繁杂，你想将它们与其他主题隔离开来。在这些场景中，可以用alterPartitionReassignments更细粒度地控制分区的每个副本要放在什么位置。需要注意的是，将副本从一个broker重新分配给另一个broker可能需要复制大量的数据。所以要注意可用的网络带宽，并在必要时使用配额限制数据复制。配额是broker级别的配置，可以用AdminClient来查看和修改它们。

假设有一个ID为0的broker。我们的主题有几个分区，每个分区都有一个副本位于这个broker上。在添加了一个新broker后，我们想用它来保存主题的一些副本，因此将通过一种略微不同的方式来分配主题分区。

```java
Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = new Hash-
Map<>();
reassignment.put(new TopicPartition(TOPIC_NAME, 0),
        Optional.of(new NewPartitionReassignment(Arrays.asList(0,1)))); ➊
reassignment.put(new TopicPartition(TOPIC_NAME, 1),
        Optional.of(new NewPartitionReassignment(Arrays.asList(1)))); ➋
reassignment.put(new TopicPartition(TOPIC_NAME, 2),
        Optional.of(new NewPartitionReassignment(Arrays.asList(1,0)))); ➌
reassignment.put(new TopicPartition(TOPIC_NAME, 3), Optional.empty()); ➍

admin.alterPartitionReassignments(reassignment).all().get();

System.out.println("currently reassigning: " +
        admin.listPartitionReassignments().reassignments().get()); ➎
demoTopic = admin.describeTopics(TOPIC_LIST);
topicDescription = demoTopic.values().get(TOPIC_NAME).get();
System.out.println("Description of demo topic:" + topicDescription); ➏
```

## 5.8　测试

Kafka提供了一个测试类MockAdminClient，你可以用它来初始化任意数量的broker，并对它执行管理操作，以便在没有真实Kafka集群的情况下测试应用程序的行为是否正确。MockAdminClient不是Kafka API的一部分，所以其发生变更时并不会告知用户，但它模拟的方法是公开的，方法签名将保持兼容。需要注意的是，这个类可能会修改和破坏你的测试，所以你需要在便利性和测试风险之间做出一些权衡。

```java
public TopicCreator(AdminClient admin) {
    this.admin = admin;
}

// 如果指定的主题名字以“test”开头，就创建这个主题
public void maybeCreateTopic(String topicName)
        throws ExecutionException, InterruptedException {
    Collection<NewTopic> topics = new ArrayList<>();
    topics.add(new NewTopic(topicName, 1, (short) 1));
    if (topicName.toLowerCase().startsWith("test")) {
        admin.createTopics(topics);

        // 为了演示而修改配置
        ConfigResource configResource =
                  new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        ConfigEntry compaction =
                  new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                          TopicConfig.CLEANUP_POLICY_COMPACT);
        Collection<AlterConfigOp> configOp = new ArrayList<AlterConfigOp>();
        configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> alterConf =
            new HashMap<>();
        alterConf.put(configResource, configOp);
        admin.incrementalAlterConfigs(alterConf).all().get();
    }
}
```

# 第6章　深入Kafka

## 6.1　集群的成员关系

Kafka使用ZooKeeper维护集群的成员信息。每个broker都有一个唯一的标识符，这个标识符既可以在配置文件中指定，也可以自动生成。

当broker与ZooKeeper断开连接（通常会在关闭broker时发生，但在发生网络分区或长时间垃圾回收停顿时也会发生）时，它在启动时创建的临时节点会自动从ZooKeeper上移除。监听broker节点路径的Kafka组件会被告知这个broker已被移除。broker对应的ZooKeeper节点会在broker被关闭之后消失，但它的ID会继续存在于其他数据结构中。例如，每个主题的副本集（参见6.3节）中就可能包含这个ID。在完全关闭一个broker后，如果使用相同的ID启动另一个全新的broker，则它会立即加入集群，并获得与之前相同的分区和主题。

## 6.2　控制器

控制器其实也是一个broker，只不过除了提供一般的broker功能之外，它还负责选举分区首领。集群中第一个启动的broker会通过在ZooKeeper中创建一个名为/controller的临时节点让自己成为控制器。其他broker在启动时也会尝试创建这个节点，但它们会收到“节点已存在”异常，并“意识”到控制器节点已存在，也就是说集群中已经有一个控制器了。其他broker会在控制器节点上创建ZooKeeperwatch，这样就可以收到这个节点的变更通知了。我们通过这种方式来确保集群中只有一个控制器。

如果控制器被关闭或者与ZooKeeper断开连接，那么这个临时节点就会消失。控制器使用的ZooKeeper客户端没有在zookeeper.session.timeout.ms指定的时间内向ZooKeeper发送心跳是导致连接断开的原因之一。当临时节点消失时，集群中的其他broker将收到控制器节点已消失的通知，并尝试让自己成为新的控制器。第一个在ZooKeeper中成功创建控制器节点的broker会成为新的控制器，其他节点则会收到“节点已存在”异常，并会在新的控制器节点上再次创建ZooKeeperwatch。每个新选出的控制器都会通过ZooKeeper条件递增操作获得一个数值更大的epoch。其他broker也会知道当前控制器的epoch，如果收到由控制器发出的包含较小epoch的消息，就会忽略它们。这一点很重要，因为控制器会因长时间垃圾回收停顿与ZooKeeper断开连接——在停顿期间，新控制器将被选举出来。当旧控制器在停顿之后恢复时，它并不知道已经选出了新的控制器，并会继续发送消息　 ——在这种情况下，旧控制器会被认为是一个“僵尸控制器”​。消息里的epoch可以用来忽略来自旧控制器的消息，这是防御“僵尸”的一种方式。

控制器必须先从ZooKeeper加载最新的副本集状态，然后才能开始管理集群元数据和执行首领选举。这个加载过程使用了异步API，为了减少延迟，请求会以管道的形式发送给ZooKeeper。但即便如此，在有大量分区的集群中，加载过程可能仍然需要几秒。

当控制器发现有一个broker离开了集群（通过观察相关的ZooKeeper路径或收到了一个来自broker的ControlledShutdownRequest）时，它知道，原先首领位于这个broker上的所有分区需要一个新首领。它将遍历所有需要新首领的分区，并决定应该将哪个分区作为新首领（简单一点儿，它可能就是副本集中的下一个副本）​。然后，它会将更新后的状态持久化到ZooKeeper中（同样，为了减少延迟，以管道的方式异步发送请求 )，再向所有包含这些分区副本的broker发送一个LeaderAndISR请求，请求中包含了新首领和跟随者的信息。出于效率方面的考虑，这些请求会被分成批次，所以每个请求都包含了多个分区最新的成员关系信息（这些分区在相同的broker上都有一个副本）​。每一个新首领都知道自己要开始处理来自生产者和消费者的请求，而跟随者也知道它们要开始从新首领那里复制消息。集群中的每一个broker都有一个MetadataCache，其中包含了一个保存所有broker和副本信息的map。控制器通过UpdateMetadata请求向所有broker发送有关首领变更的信息，broker会在收到请求后更新缓存。在启动broker副本时也会有类似的过程——主要的区别是broker所有的分区副本都是跟随者，并且需要在自己有资格被选为首领之前与首领保持同步。总的来说，Kafka会使用ZooKeeper的临时节点来选举控制器，并会在broker加入或退出集群时通知控制器。控制器负责在broker加入或退出集群时进行首领选举。控制器会使用epoch来避免“脑裂”​。所谓的“脑裂”​，就是指两个broker同时认为自己是集群当前的控制器。

__新控制器KRaft__

2019年，Kafka社区启动了一个雄心勃勃的项目：使用基于Raft的控制器替换基于ZooKeeper的控制器。新控制器叫作KRaft，其预览版包含在Kafka 2.8中。于2021年9月发布的Kafka 3.0包含了它的第一个生产版本，Kafka集群既可以使用基于ZooKeeper的传统控制器，也可以使用KRaft。

## 6.3　复制

复制是Kafka架构核心的一部分。Kafka经常被描述成“一个分布式、分区、可复制的提交日志服务”​。复制之所以这么重要，是因为它可以在个别节点失效时仍能保证Kafka的可用性和持久性。前面介绍过，Kafka的数据保存在主题中，每个主题被分成若干个分区，每个分区可以有多个副本。副本保存在broker上，每个broker可以保存成百上千个主题和分区的副本。

__首领副本__

每个分区都有一个首领副本。为了保证一致性，所有生产者请求和消费者请求都会经过这个副本。客户端可以从首领副本或跟随者副本读取数据。

__跟随者副本__

首领以外的副本都是跟随者副本。如果没有特别指定，则跟随者副本将不处理来自客户端的请求，它们的主要任务是从首领那里复制消息，保持与首领一致的状态。如果首领发生崩溃，那么其中的一个跟随者就会被提拔为新首领。

KIP-392中加入了从跟随者副本读取数据的特性。这个特性的主要目的是允许客户端从最近的同步副本而不是首领副本读取数据，以此来降低网络流量成本。要使用这个特性，消费者端需要配置可以标识客户端位置的client.rack。broker端需要配置replica.selector.class，默认为LeaderSelector（表示总是从首领副本读取数据）​，可以把它设置为RackAwareReplicaSelector，它将选择一个最近的副本，这个副本所在的broker的rack.id与客户端配置的client.rack相匹配。也可以实现ReplicaSelector接口，使用自定义副本选择逻辑。

首领的另一项任务是搞清楚哪些跟随者副本的状态与自己是一致的。为了保持与首领同步，跟随者会尝试从首领那里复制消息，但它们可能会因为各种原因无法与首领保持同步。例如，网络拥塞导致复制变慢，或者broker发生崩溃并重启，都将导致所有副本的复制滞后。为了与首领保持同步，跟随者需要向首领发送Fetch请求，这与消费者为了读取消息而发送的请求是一样的。作为响应，首领会将消息返回给跟随者。Fetch请求消息里包含了跟随者想要获取的消息的偏移量，这些偏移量总是有序的。这样，首领就可以知道一个副本是否已经获取了最近一条消息之前的所有消息。通过检查每个副本请求的最后一个偏移量，首领就可以知道每个副本的滞后程度。如果副本没有在30秒内发送请求，或者即使发送了请求但与最新消息的间隔超过了30秒，那么它将被认为是不同步的。如果一个副本未能跟上首领，那么一旦首领发生故障，它将不能再成为新首领——毕竟，它并未拥有所有的消息。与此相反，持续发出获取最新消息请求的副本被称为同步副本。当首领发生故障时，只有同步副本才有资格被选为新首领。允许跟随者可以多久不活跃或允许跟随者在多久之后成为不同步副本是通过replica.lag.time.max.ms参数来配置的。这个时间直接影响首领选举期间的客户端行为和数据保留机制。

除了当前的首领，每个分区都有一个首选首领，即创建主题时选定的首领。之所以是首选的，是因为在创建分区时，分区首领在broker间的分布已经是均衡的。因此，我们希望当首选首领成为当前首领时，broker之间的负载是均衡的。在默认情况下，Kafka的auto.leader.rebalance.enable会被设置为true，它会检查首选首领是不是当前首领以及是不是同步的。如果是同步的，但不是当前首领，就会触发首领选举，让首选首领成为当前首领。

## 6.4　处理请求

broker的大部分工作是处理客户端、分区副本和控制器发送给分区首领的请求。Kafka提供了一种二进制协议（基于TCP）​，指定了请求消息的格式以及broker如何对请求做出响应——既包括成功处理请求，也包括在处理请求过程中出现错误。Kafka项目提供了由Kafka项目贡献者实现和维护的Java客户端，除此之外，还有其他使用不同的编程语言（比如C语言、Python、Go语言等）开发的客户端。你可以在Kafka官方网站上看到完整的清单。这些客户端都使用这个协议与broker通信。客户端总是发起连接并发送请求，而broker负责处理这些请求并做出响应。broker会按照请求到达的顺序来处理它们，这种顺序既能保证让Kafka具备消息队列的特性，又能保证保存的消息是有序的。

broker会在它监听的每一个端口上运行一个接收器线程，这个线程会创建一个连接，并把它交给处理器线程处理。处理器线程（也叫网络线程）的数量是可配置的。网络线程负责从客户端获取请求，把它们放进请求队列，然后从响应队列取出响应，把它们发送给客户端。有时候，服务器端需要延迟对客户端做出响应，例如，消费者要求只在有可用数据时接收响应，或者发出DeleteTopic请求的客户端要求在开始删除主题之后才接收响应。延迟的响应会被放在炼狱（临时内存）中，直到它们可以被发送给客户端

![avatar](img/13.png)

请求消息被放入请求队列后，IO 线程（也叫请求处理线程）会负责处理它们。下面是几种最常见的请求类型。

__生产请求__

生产者发送的请求，其中包含了客户端要写入broker的消息。

__获取请求__

消费者和跟随者副本发送的请求，用于从broker读取消息。

__管理请求__

管理客户端发送的请求，用于执行元数据操作，比如创建和删除主题。

生产请求和获取请求都必须发送给分区的首领。如果broker收到一个针对某个分区的写入请求，而这个分区的首领在另一个broker上，那么发送请求的客户端将收到“非分区首领”错误响应。如果针对某个分区的读取请求被发送到一个不包含这个分区首领的broker上，那么也会收到同样的错误。Kafka客户端负责把生产请求和获取请求发送到包含分区首领的broker上。

那么客户端怎么知道该向哪里发送请求呢？客户端使用了另一种请求类型，也就是元数据请求，请求中包含了客户端感兴趣的主题清单。这种请求的响应消息里指明了这些主题所包含的分区、每个分区都有哪些副本，以及哪个副本是首领。元数据请求可以被发送给任意一个broker，因为所有broker都缓存了这些元数据信息。一般情况下，客户端会把这些信息缓存起来，并直接向目标broker发送生产请求和获取请求。它们需要时不时地通过发送元数据请求来刷新缓存（刷新的时间间隔可以通过metadata.max.age.ms参数来配置）​，以便知道元数据是否发生了变化。

![avatar](img/14.png)

### 6.4.1　生产请求

首先，包含某个分区首领的broker在收到生产请求时会对请求做一些验证。

● 发送数据的用户是否有主题写入权限？


● 请求中指定的acks是否有效（只允许出现0、1或all）​？

● 如果acks=all，那么是否有足够多的同步副本保证消息已经被安全写入？

然后，消息将被写入本地磁盘。在Linux系统中，消息会被写入文件系统缓存，但不能保证何时会被冲刷到磁盘上。Kafka不会一直等待数据被持久化到磁盘上，它主要通过复制功能来保证消息的持久性。

一旦消息被写入分区的首领，broker就会检查acks配置参数——如果acks是0或1，那么broker就会立即返回响应；如果acks是all，则请求将被保存在一个叫作炼狱的缓冲区中，直到首领确认跟随者副本复制了消息，才将响应返回给客户端。

### 6.4.2　获取请求

broker处理获取请求的方式与处理生产请求的方式很相似。客户端发送请求，希望broker返回指定主题、分区和特定偏移量位置的消息，就好像在说：​“请把主题Test的分区0中偏移量从53开始的消息以及主题Test的分区3中偏移量从64开始的消息发给我。​”客户端还可以指定一个分区最多可以返回多少数据。这个限制非常重要，因为客户端需要为broker返回的数据分配足够的内存。如果没有这个限制，并且broker返回了大量的数据，则可能会耗尽客户端的内存。之前说过，请求需要发送给指定的分区首领，所以客户端需要通过查询元数据来确保请求被路由到正确的节点上。首领在收到请求时会先检查请求是否有效，比如，指定的偏移量在分区中是否存在？如果客户端请求的数据已被删除，或者请求的偏移量不存在，则broker会返回错误。如果请求的偏移量存在，那么broker将按照客户端指定的数量上限从分区中读取消息，再把消息返回给客户端。Kafka使用零复制技术向客户端发送消息，也就是说，Kafka会直接把消息从文件（或者更确切地说是Linux文件系统缓存）里发送到网络通道，不需要经过任何中间缓冲区。这是Kafka与其他大部分数据库系统不一样的地方，其他数据库在将数据发送给客户端之前会先把它们保存在本地缓存中。这项技术避免了字节复制，也不需要管理内存缓冲区，从而能够获得更好的性能。

除了可以设置broker返回数据的上限，客户端也可以设置broker返回数据的下限。如果把下限设置为10 KB，就好像是在告诉broker“等到有10 KB数据时再把它们返回给我”​。在主题消息流量不是很大的情况下，这样可以减少CPU和网络开销。具体操作如下：客户端发送一个请求，broker在等到有足够数据时才把它们返回给客户端，然后客户端再发起另一个请求，而不是让客户端每隔几毫秒就发送一次请求，可能每次都只能拿到很少的数据，甚至没有数据（参见图6-3）​。对比这两种情况，虽然它们最终读取的数据总量是一样的，但前者的来回传送次数更少，开销也更小。

![avatar](img/15.png)

有意思的是，并不是所有保存在分区首领上的数据都可以被客户端读取。大部分客户端只能读取已经被写入所有同步副本 [ 跟随者副本除外（尽管它们也是消费者）​，否则复制功能将无法正常工作 ] 的消息。分区首领知道哪些消息已经被复制到哪些副本上，所以消息在还没有被写入所有同步副本之前是不会被发送给消费者的——尝试获取这些消息的请求会得到空响应，而不是错误。

之所以这样，是因为还没有被足够多分区副本复制的消息被认为是“不安全”的——如果首领发生崩溃，另一个副本成为新首领，那么这些消息就丢失了。

![avatar](img/16.png)

### 6.4.3　其他请求

## 6.5　物理存储

Kafka的基本存储单元是分区。分区既无法在多个broker间再细分，也无法在同一个broker的多个磁盘间再细分。所以，分区的大小受单个挂载点可用空间的限制。​

在配置Kafka时，管理员会指定一个用于保存分区数据的目录列表，也就是log.dirs参数（不要把它与存放错误日志的目录混淆了，日志目录是配置在log4j.properties文件中的）​。这个参数一般会包含Kafka将要使用的每一个挂载点的目录。

### 6.5.1　分层存储






