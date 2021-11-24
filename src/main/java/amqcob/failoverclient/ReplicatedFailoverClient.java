package amqcob.failoverclient;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.net.Socket;
import java.util.Scanner;


public class ReplicatedFailoverClient {

    public static void main(final String[] args) throws Exception {
        final int numMessages = 20;
        Connection connection = null;
        InitialContext initialContext = null;

        try {
            initialContext = new InitialContext();

            Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");
            ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

            connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            connection.start();

            MessageConsumer consumer = session.createConsumer(queue);

            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println("############################################");
            System.out.println();
            System.out.println("Receiving and acknowledging a third of the sent messages");

            TextMessage message0 = null;
            for (int i = 0; i < numMessages / 3; i++) {
                message0 = (TextMessage) consumer.receive(50000);
                System.out.println("Got message: " + message0.getText());
            }
            message0.acknowledge();

            System.out.println();
            System.out.println();
            System.out.println("Receiving the rest third of messages but do not acknowledge them yet");
            for (int i = numMessages / 3; i < numMessages; i++) {
                message0 = (TextMessage) consumer.receive(500000);
                System.out.println("Got message: " + message0.getText());
            }

            checkBroker();
            readInput("Are you ready to test the failover? (y/n)");

            System.out.println("Acknowledging the 2nd half of the sent messages");
            try {
                message0.acknowledge();
            }catch (JMSException e) {
                System.out.println("Got the expected exception while acknowledging the message: " + e.getMessage());
            }

            System.out.println();
            System.out.println("Consuming the remaining messages");
            for (int i = numMessages / 3; i < numMessages; i++) {
                message0 = (TextMessage) consumer.receive(500000);
                System.out.println("Got message: %s\n" + message0.getText());
            }
            message0.acknowledge();
        } finally {

            if (connection != null) {
                connection.close();
            }

            if (initialContext != null) {
                initialContext.close();
            }
        }
    }


    public static void checkBroker() {
        boolean stopped = false;
        System.out.println();
        System.out.println();
        do {
            try (Socket ignore = new Socket("localhost",61617)) {
                stopped = false;
                System.out.println("Broker1 is running. Please stop it !!!");
                Thread.sleep(3000);
            } catch (Exception ignored) {
                stopped = true;
            }
        }while(!stopped);

        System.out.println();
        System.out.println();
    }

    public static void readInput(String message) {
        Scanner scanner = new Scanner(System.in);
        String response = null;
        boolean ok = false;

        do {
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.flush();
            System.out.println(message);
            try {
                if(scanner.hasNextLine()) {
                    response = scanner.nextLine();
                    response = response.toLowerCase();
                }
            }catch (Exception e){}

            if("y".equals(response) || "yes".equals(response)) {
                ok = true;
            }
        }while(!ok);
        scanner.close();
    }
}

