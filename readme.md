# RabbitMQ 从入门到入土

https://developer.aliyun.com/article/769883

https://blog.csdn.net/lzx1991610/article/details/102970854



**教程开始之前需要先安装 RabbitMQ，参考**[**https://blog.csdn.net/qq_64195455/article/details/141596990?spm=1001.2014.3001.5501**](https://blog.csdn.net/qq_64195455/article/details/141596990?spm=1001.2014.3001.5501)



# Queues

## 一、Hello World（一个生产者一个消费者）

消息队列是一个消息中间人，负责接收消息，存储消息，转发消息

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730258822698-e8d816d9-67e0-41a6-bc07-26127b41b0b1.png)

一些术语（jargon）：

- 生产者：发送消息的程序称为生产者
- 队列：类似邮箱，用来存储消息，本质是一个大的消息缓冲区
- 消费者：等待接收消息的程序称为消费者



生产者、消费者、队列不一定需要在同一个机器上，一个应用程序也可以既是消费者，又是生产者。



## ![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1725323434764-ab691b1a-5d61-40ba-a611-90c259be6406.png)



创建一个 maven 项目，导入相应的依赖

```xml
<dependencies>
        <!-- https://mvnrepository.com/artifact/com.rabbitmq/amqp-client -->
        <dependency>
            <groupId>com.rabbitmq</groupId>
            <artifactId>amqp-client</artifactId>
            <version>5.21.0</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-api -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.16</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-simple -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.16</version>
            <scope>test</scope>
        </dependency>

    </dependencies>
```



编写生产者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Send {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        // 建立连接，是一个抽象的连接
        // 这里做了很多的封装和抽象
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = "Hello World!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
```

生产者连接 RabbitMQ 发送消息

为什么需要channel:

通道提供了对底层TCP连接的抽象。通过使用通道，应用程序可以发送和接收消息，而无需直接处理底层的socket连接和网络协议。  



注意到获取连接时，使用了 **工厂模式**

工厂模式参考：

https://github.com/bytesfly/blog/blob/master/DesignPattern/factory-pattern.md

https://blog.csdn.net/a745233700/article/details/120253639

https://blog.csdn.net/A_hxy/article/details/105758657



消费者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Recv {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for message. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});

    }
}
```



**声明一个队列是幂等的——只有当他不存在时才创建**

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730430922994-b7edd67c-8b70-44a0-b53a-5fd3ad6d177d.png)



为什么不使用 `try-with-resource`

因为我们希望消费者持续监听队列，当队列有消息就进行消费。而不像消费者那样发送一个消息然后就结束了。



为什么消费者要声明队列？

因为生产者和消费者往往是两个不同的服务，可能存在生产者还没启动，即还没创建队列，消费者已经启动了，并且开始监听，为什么保证监听的队列存在，需要先声明和生产者具有一样参数的队列。



为什么要定义 `DeliverCallback` 回调函数

`DeliverCallback` 允许我们定义一个处理逻辑，当有新消息到达时，这个回调函数会被触发，帮助我们有效地处理接收到的消息。  

起到一个对消息的缓冲作用。

通过实现`DeliverCallback`接口，我们可以在回调方法中编写逻辑来处理和存储消息。这样可以确保即使在接收消息的过程中，我们的应用程序可以保持其他操作的流畅性，不会因为消息的处理而阻塞。  

# 二、工作队列（work Queues）（一个生产者多个消费者）

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1725327438924-fa46d862-6774-492a-a1ec-dce4d13241fa.png)

**工作队列（任务队列）的核心思想就是避免立即执行资源密集型任务并等待它完成，而是把它放到一个队列中，等待消费者去消费。**

在BI项目中，AI就是一个资源密集型且长耗时的任务，所以可以使用消息队列进行异步。

比如电商项目，订单系统要调用库存系统和支付系统，也可以说是一个长耗时的任务，就可以进行异步。

等任务处理完成再进行通知

这也解决了HTTP 短连接无法处理复杂问题的问题

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730431560694-a155953f-73d4-49a9-9e8a-e12833ef17a8.png)



**工作队列模式的优点：可以并行处理任务（多消费者）**

## 轮询分配（Round-robin dispatching）

默认情况下，RabbitMQ 会按顺序将每条消息发送给下一个消费者。平均而言，每个消费者都会收到相同数量的信息。这种分发消息的方式称为轮询。

假设有2个消费者，发送5条消息，那么一个消费者将收到3条，一个收到2条，并且正常是满足奇偶性。

## 消息确认(Message acknowledgment)

**为了确保消息不丢失，RabbitMQ 支持消息确认。**

消费者将发送一个确认，告诉 RabbitMQ 消息已经被接收和处理，可以放心的删除。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730618110511-2e3e3c40-c2db-42b0-b1b3-0286cb719dc5.png)



如果消费者挂了（channel、connection关闭了），没有发送 ack, RabbitMQ 会认为消息未被完全处理，并把消息重新入队，如果这时候有其他消费者在线，会快速发送给请他消费者。保证消费者挂了，也不会丢失信息。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730618424771-b3017577-526d-4200-bcb1-e7be5a9704ca.png)



默认情况下30min内消费者必须传递ack, 超过这个时间则认为消费者卡住了（bug），这个时间可以调整（https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout）

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730618669143-445f9f4e-d615-4c5c-a2e7-54966338bcab.png)



**默认情况下是需要手动进行消息确认的。**

之前的代码我们是修改 `autoAck=true`来实现自动确认的，现在我们把他关掉（改成 `false`), 然后手动进行消息确认。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730619109587-2374d83d-c092-478c-85ed-040b1e5f0daf.png)



**消息确认必须在同一个channel，使用不同channel会抛异常。**

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730620011997-7972e8b5-6383-45d6-94f2-7cc4fc5106aa.png)



## 消息持久化（Message durability）

消息确认机制确保消费者挂了不会丢失任务，如果消息队列挂了怎么办？



为了解决这个问题，我们需要让**队列和消息持久化**



**队列持久化(重启之后队列还在)：**

```java
boolean durable = true;
channel.queueDeclare("hello", durable, false, false, null);
```

上面的命令没错，但是执行会出错。

因为 `hello队列`前面我们已经定义过了。**RabbitMQ 不允许你用不同的参数重新定义一个已存在的队列。**

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730621499660-e51dddb5-b401-47c6-8848-8461f1f5166f.png)

解决办法是更换名字：

```java
boolean durable = true;
channel.queueDeclare("task_queue", durable, false, false, null);
```

**消息持久化：**

设置 `MessageProperties`为 `PERSISTENT_TEXT_PLAIN`

```java
import com.rabbitmq.client.MessageProperties;

channel.basicPublish("", "task_queue",
            MessageProperties.PERSISTENT_TEXT_PLAIN,
            message.getBytes());
```

**注意：**

即使设置了消息持久化，也不能保证完全不会丢失，消息队列接收消息并保存消息会有一个短的时间窗口，这段时间可能丢失信息，而且消息队列可能不会立即就保存到磁盘，而是保存在缓冲区。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730622148333-d51b3b24-d1ca-4ec7-a6e4-1c19dd839f28.png)



## 公平分配（Fair dispatch）

您可能已经注意到调度仍然没有完全按照我们希望的那样工作。例如，在两个worker的情况下，当所有奇数消息都很重，偶数消息很轻时，一个worker将一直很忙，而另一个几乎不做任何工作。好吧，RabbitMQ对此一无所知，仍然会均匀地分发消息。



消息队列负责传递消息给消费者，而不关心消费者未确认的消息数量。它只是盲目地把消息按照轮询的方式分配给消费者。



为了解决这个问题，我们可以设置 `basicQos(1)`。这将告诉 RabbitMQ 一次不要给 消费者 发送多个消息。在前一条消息确认之前，不会发送新的消息。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730623028232-06ce97c9-cf14-4202-9fce-d42606da6052.png)

```java
int prefetchCount = 1;
channel.basicQos(prefetchCount);
```

注意：

这样的设置的话，当所有 消费者 都繁忙的时候，消息会放在队列里面，这样队列可以会满，此时你需要采取其他策略。



生产者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.util.Scanner;

public class NewTask {
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            // 官方这里是使用命令行传递参数
            // 这里改用 scanner 使用 maven 作为依赖不方便使用命令行
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String message = scanner.nextLine();

                channel.basicPublish("", TASK_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }
}
```

消费者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Worker {
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for message. To exit press CTRL+C");

        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            try {
                System.out.println(" [x] Received '" + message + "'");
                // 睡眠20 s, 模拟长耗时任务
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // 开启消费监听
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> {
        });
    }
}
```



# 三、发布订阅（Publish/Subscribe）

工作队列模型每条消息只会发送给一个消费者

发布订阅模型是把一条消息发送给多个消费者

发布订阅，也可以叫广播（broadcast），一个发送消息，广播到所有接收者。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730624727368-2776226a-4d1e-4dbd-8772-f9a233856f3e.png)



## 交换机（Exchanges）

在前面的学习中，我们只是向队列发送消息和取消息，现在介绍完整的Rabbit 消息模型。

回顾前面的模型：

- Producer
- Queue
- Consumer

**RabbitMQ消息模型的核心思想是生产者永远不会直接向队列发送任何消息。实际上，通常生产者甚至不知道消息是否将被传递到任何队列。**

**生产者只能向交换机发送消息。交换机负责将消息转发到各个队列，可以是某个特定的队列，也可以是多个队列，甚至可以丢弃。**

**这些转发规则由交换机的类型决定。**

**总之，交换机必须确切地知道如何处理它接收到的消息。**



![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730625276316-0fbfb6ab-11ef-4ff6-874b-41bc4c422d3c.png)

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730625527832-5aa7d3f0-d0be-48ae-8675-972482ec9efa.png)

交换机的类型：

- `direct`
- `topic`
- `headers`
- `fanout`



接下来，我们介绍 `fanout`类型，因为它对应发布订阅模型，后面的类型将在后面的章节介绍。

```java
channel.exchangeDeclare("logs", "fanout");
```

我们声明一个 `fanout`类型的交换机并命名为 `logs`，`fanout`类型的交换机会将消息发送给所有它知道的队列。



在前面的例子中，我们并没有提到交换机，但我们仍然可以发送消息，这是因为我们使用了默认的交换机，它使用（""）标识

```java
channel.basicPublish("", "hello", null, message.getBytes());
```

第一个参数就是交换机的名称，`""`代表默认交换机或者叫匿名交换机，如果有 routingKey 将会把消息转发到具有该 routingKey 的队列



使用我们前面定义的 `logs`交换机

```java
channel.basicPublish( "logs", "", null, message.getBytes());
```

## 临时队列（Temporary queues）

当我们在连接到 RabbitMQ  的时候需要创建一个新的空的队列的时候，并且独占、并且最好有一个随机名称，即不和以前的队列冲突，并且在连接关闭时，自动删除队列的话，我们可以使用临时队列。

当我们不向 `queueDeclare()`提供参数时，就会创建一个非持久化、独占的、自动删除的队列。

```java
String queueName = channel.queueDeclare().getQueue();
```



## 绑定（Bindings）

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730634742294-5cf9985f-a575-403c-a929-f06f2a96397d.png)

我们需要告诉交换机发送消息到哪个队列。关联交换机和队列称为绑定（binding）

```java
channel.queueBind(queueName, "logs", "");
```

对于 `fanout`交换机， `routingKey`是没用的，所以为 `""`



生产者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class EmitLog {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            Scanner scanner = new Scanner(System.in);

            while (scanner.hasNext()) {
                String message = scanner.nextLine();
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }
}
```

消费者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogs {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for message. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
```

可以发现，在声明完 `channel` 之后，我们需要声明 `exchange`，这一步是必要的，因为禁止向不存在的交换机发布消息。



# 四、路由（Routing）

回顾前面我们绑定队列和交换机的代码：

```java
channel.queueBind(queueName, EXCHANGE_NAME, "");
```

上面我们并没有指定 `routingKey`，因为对于 `fanout`类型的交换机，`routingKey`是没有作用的，所以绑定还和交换机的类型相关。

下面创建一个绑定：

```java
channel.queueBind(queueName, EXCHANGE_NAME, "black");
```



## Direct exchange

直接交换机的路由算法：**把消息发送到** `**routingKey**`**完全匹配的队列**

为了进一步说明，一图胜千言：

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730687199653-b912379f-a4fb-4ad2-9a15-a3aba9dafb6b.png)

Q1 绑定了 `orange`

Q2 绑定了 `black`和 `green`

具有`orange`的路由键的消息会被转发到 `Q1`

具有 `black`或 `green`的会转发到 `Q2`

其他的消息将丢弃



## Multiple bindings

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730687597680-0d6f959d-f1c6-4261-a2d7-4ee026649242.png)

相同的`routingKey`绑定到多个队列是合法的，如上图，`routingKey`为 `black`的消息将被转发到 `Q1`和 `Q2`



## 例子

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730688090184-eea7638f-cc04-4be3-a8ce-c1daadfcbeb3.png)

具有`error`的`routingKey`消息转发到 `Q1`和 `Q2`

具有 `info` 和 `warn`转发到 `Q2`



生产者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            Scanner scanner = new Scanner(System.in);
            String[] split = scanner.nextLine().split(" ");
            String severity = split[0];
            String message = split[1];

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }
}
```

消费者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        // 创建两个临时队列
        String queueName1 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName1, EXCHANGE_NAME, "error");

        String queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, EXCHANGE_NAME, "error");
        channel.queueBind(queueName2, EXCHANGE_NAME, "warn");
        channel.queueBind(queueName2, EXCHANGE_NAME, "info");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName1 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName2 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        channel.basicConsume(queueName1, true, deliverCallback1, consumerTag -> { });
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> { });
    }
}
```

# 五、主题（Topics）

为了进一步增加日志监听的灵活性（复杂性），学习 `Topic exchange`



## Topic exchange

发送到主题交换机的消息的 `routingKey`有一些限制：它必须是一个用点分隔开的单词列表，如：`stock.usd.nyse`，`nyse.vmw`，`quick.orange.rabbit`等

整个 `routingKey`的长度不超过 255 字节

**上面是消息的** `**routingKey**`**的格式，下面介绍队列的** `**routingKey**`**的格式**

队列的 `routingKey`可以使用 **通配符** 进行模糊匹配，相比 `direct`的精确匹配更加灵活

- `*` 匹配一个单词
- `#`匹配 0 个 或 多个 单词

上图：

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730691558674-20e0eb19-4151-4e54-9316-c5546ab9e449.png)

我们将发送一些描述动物的消息

消息带有 `routingKey`，每个 `routingKey`由三个单词（两个点）组成。第一个单词描述速度，第二个单词描述颜色，第三个单词描述物种

有两个队列：

- `Q1`绑定 `*.orange.*`
- `Q2`绑定 `*.*.rabbit` 和 `lazy.#`

总结：

- `Q1`匹配 orange 颜色的动物
- `Q2`匹配所有和兔子相关的以及 lazy类型的动物

例子：

- `quick.orange.rabbit` 转发到 `Q1`和 `Q2`
- `lazy.orange.elephant`转发到 `Q1`和 `Q2`
- `quick.orange.fox`转发到 `Q1`
- `lazy.brown.fox`转发到`Q2`
- `lazy.pink.rabbit`转发到 `Q2`，尽管匹配两个 `RoutingKey`但是也只会转发一次
- `quick.brown.fox`没有匹配，将丢弃

不满足三个单词的例子：

- `orange`丢弃
- `quick.orange.new.rabbit` 丢弃
- `lazy.orange.new.rabbit`转发到 `Q2`

主题交换机可以模拟其他交换机

当 `routingKey`设置为 `#`时，可以模拟 `fanout`交换机

当 `routingKey`不含通配符(`*`和 `#`) 时，可以模拟 `Direct`交换机

生产者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class EmitLogTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String[] split = scanner.nextLine().split(" ");

                String routingKey = split[0];
                String message = split[1];

                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
            }
        }
    }
}
```

消费者代码：

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // 创建多个临时队列
        String queueName1 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName1, EXCHANGE_NAME, "#");

        String queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, EXCHANGE_NAME, "kern.*");

        String queueName3 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName3, EXCHANGE_NAME, "*.critical");

        String queueName4 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName4, EXCHANGE_NAME, "kern.*");
        channel.queueBind(queueName4, EXCHANGE_NAME, "*.critical");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName1 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName2 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        DeliverCallback deliverCallback3 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName3 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        DeliverCallback deliverCallback4 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName4 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        channel.basicConsume(queueName1, true, deliverCallback1, consumerTag -> { });
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> { });
        channel.basicConsume(queueName3, true, deliverCallback3, consumerTag -> { });
        channel.basicConsume(queueName4, true, deliverCallback4, consumerTag -> { });
    }
}
```



# 六、可靠发布之发布确认（Publisher Confirms）

发布确认可以确保生产者发送的消息安全抵达到RabbitMQ。

下面将介绍几种使用发布确认的策略以及利弊

## 开启 `Publisher Confirms`

发布确认是对 AMQP 协议的扩展，所以默认不开启。

开启如下：

```java
Channel channel = connection.createChannel();
channel.confirmSelect();
```

 此方法应在每个需要使用发布者确认的频道上调用，仅需调用一次，而不是为每条消息都调用。  



## 策略1：单独发布消息（Publishing Messages Individually）

最简单的模式：生产者发布一条消息然后同步等待消息确认。

```java
while (thereAreMessagesToPublish()) {
    byte[] body = ...;
    BasicProperties properties = ...;
    channel.basicPublish(exchange, queue, properties, body);
    // uses a 5 second timeout
    channel.waitForConfirmsOrDie(5_000);
}
```

使用 `waitForConfirmsOrDie(long)`方法实现超时等待，当超过设置的时间消息还没有确认或者消息被破坏，该方法就会抛出异常。



这种方式简单，但是有一个缺点：降低了发布消息的性能。吞吐量将明显下降。

消息确认真的是异步的吗？

上面的方法看似是同步等待消息确认，但实际上生产者接收确认是异步的，内部依赖于异步通知。

![img](https://cdn.nlark.com/yuque/0/2024/png/43895563/1730709721651-b8acc363-9b95-497e-8921-eed7a45299ff.png)



完整代码：

```java
static void publishMessagesIndividually() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();

            String queueName = UUID.randomUUID().toString();
            channel.queueDeclare(queueName, false, false, true, null);

            channel.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", queueName, null, body.getBytes());
                channel.waitForConfirms(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }
```

## 策略2：批量发布消息（Publishing Message in Batches）

对前面的程序进行改进，批量发布消息并且等待整批消息得到确认。

下面以100为一批

```java
int batchSize = 100;
int outstandingMessageCount = 0;
while (thereAreMessagesToPublish()) {
    byte[] body = ...;
    BasicProperties properties = ...;
    channel.basicPublish(exchange, queue, properties, body);
    outstandingMessageCount++;
    if (outstandingMessageCount == batchSize) {
        channel.waitForConfirmsOrDie(5_000);
        outstandingMessageCount = 0;
    }
}
if (outstandingMessageCount > 0) {
    channel.waitForConfirmsOrDie(5_000);
}
```

批量发送显著提高了性能，但是也有一个缺点，就是当出现故障的时候，你无法定位到这一批消息中是哪一个出了问题。解决办法是把这批消息保存到内存中，以及记录一些有意义的内容或者从新发送整批消息。

完整代码：

```java
static void publishMessageInBatch() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue, false, false, true, null);

            channel.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    channel.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }
```

## 策略3：异步处理发布确认（Handling Publisher Confirms Asynchronously)

消息队列异步确认发布消息，注册一个回调就可以收到这些确认的通知：

```java
Channel channel = connection.createChannel();
channel.confirmSelect();
channel.addConfirmListener((sequenceNumber, multiple) -> {
    // code when message is confirmed
    // 当消息被确认时的处理代码
}, (sequenceNumber, multiple) -> {
    // code when message is nack-ed
    // 当消息被拒绝（nack）时的处理代码
});
```

有两个回调 一个确认消息，一个丢失消息。

每个回调都要两个参数

- **sequence number**：一个标识符，用于确认或拒绝的消息。  
- **multiple**：布尔值，表示是单个消息的确认（`false`）还是多个消息的确认（`true`），即所有序列号小于或等于该序列号的消息。  

序列号（sequence number）获取通过 `Channel#getNextPublishSeqNo()`

```java
int sequenceNumber = channel.getNextPublishSeqNo());
ch.basicPublish(exchange, queue, properties, body);
```

使用 Map 将消息与序列号关联起来

```java
ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();
// ... code for confirm callbacks will come later
String body = "...";
outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
channel.basicPublish(exchange, queue, properties, body.getBytes());
```

当消息被确认只后，需要从 Map 中移除

```java
ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
    if (multiple) {
        // 清理多个确认的消息
        ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
        confirmed.clear();
    } else {
        // 清理单个确认的消息
        outstandingConfirms.remove(sequenceNumber);
    }
};
```

当消息被拒绝时，会获取消息的内容并记录警告，同时也会调用确认回调即上面的回调，从 Map 中移除

```java
channel.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
    String body = outstandingConfirms.get(sequenceNumber);
    System.err.format("Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n", body, sequenceNumber, multiple);
    cleanOutstandingConfirms.handle(sequenceNumber, multiple);
});
```

**总之，无论消息被确认还是被拒绝都需要从Map 中清除**

如何跟踪未完成确认的消息？

使用 `ConcurrentNavigableMap`

理由：

- 能够把 序列号 和 消息 进行关联
- 能够确认到 某个特定的序列号id （支持处理多个）（说人话就是支持 lowerbound 的范围处理）
- 线程安全（concurrent 开头的集合都是安全的）



综上所述，异步处理发布确认通常包含以下步骤：

- 把消息和序列号进行关联
- 在通道上设置确认监听器，以便在收到（ack）和 拒绝（nack）时得到通知，同时还有执行适当的操作，例如记录日志或者重新发布拒绝的消息。在次步骤中，以及确认的消息需要在 Map 进行清理。
- 发布消息之前，需要有一个序列号进行跟踪

需要重新发布拒绝的消息吗？

在回调中重新发送拒绝的消息可能很诱人，但是官方不推荐

因为确认回调在 I/O 线程中运行，通常不允许在此线程中进行通道操作

更好的做法：

- 把被拒绝的消息加入到一个内存队列中（如：`ConcurrentLinkedQueue`）
- 设置一个单独的发布线程来轮询该队列，以处理消息的重新发布，从而确保操作发生在适当的线程上下文中



## 总结

在某些应用中，确认发布的消息到达 RabbitMQ broker 十分重要。RabbitMQ 的发布确认功能可以满足这个需求。发布确认可以是异步的，也可以是同步的。没有一种固定的方式来实现发布确认，通常取决于应用的需求和系统的整体约束。常见的策略如下：

1. 单条消息逐个发布，同步等待确认：

   - 优点：简单易懂

   - 缺点：吞吐量非常有限


2. 批量发布消息，并同步等待批量确认

   - 优点：相对简单，吞吐量也还行

   - 缺点：出错了很难处理，因为无法精确定位到那一条消息


3. 异步处理

   - 优点：提供最佳的性能和资源利用，能够在出错时提供良好的控制

   - 缺点：实现起来复杂