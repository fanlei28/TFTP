package lf.tftp;

import java.io.IOException;
import java.net.*;

public class Client {
    private static final int TFTP_PORT = 69;
    private static final short WRQ_OPCODE = 2;
    private static final short DATA_OPCODE = 3;
    private static final short ACK_OPCODE = 4;
    private static final int ACK_PACKET_SIZE = 4;

    private static final int MAX_PACKET_SIZE = 512;

    private static final int DATA_PACKET_MAX_BYTES_COUNT = 512;
    private static final int DATA_PACKET_BYTES_OFFSET = 4;
    private static final int DATA_PACKET_MAX_SIZE = DATA_PACKET_MAX_BYTES_COUNT + DATA_PACKET_BYTES_OFFSET;

    public static void main(String[] args) throws Exception {
        sendPackets(10, "/Users/lei/Documents/hello.txt");
    }

    static void sendPackets(int max, String filename) {
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] req = createWriteRequest(filename);
            DatagramPacket packet = new DatagramPacket(req, req.length, InetAddress.getLocalHost(), TFTP_PORT);
            socket.send(packet);

            short blockNum = (short) 1;
            String data = "helloworldworldworldworldworldworldworld";

            DatagramPacket recPacket;

            while (blockNum <= max) {
                recPacket = new DatagramPacket(new byte[MAX_PACKET_SIZE], MAX_PACKET_SIZE);
                socket.receive(recPacket);
                printPacket(recPacket);

                byte[] recData = recPacket.getData();
                short recBlockNum = (short) ((recData[2] << 8) | recData[3]);
                assert (recBlockNum == blockNum);

                byte[] dataBytes = createWriteData(DATA_PACKET_MAX_SIZE, blockNum, data);
                DatagramPacket sendPacket = new DatagramPacket(dataBytes, dataBytes.length, recPacket.getAddress(), recPacket.getPort());
                blockNum++;

                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                socket.send(sendPacket);
            }

            recPacket = new DatagramPacket(new byte[MAX_PACKET_SIZE], MAX_PACKET_SIZE);
            socket.receive(recPacket);
            printPacket(recPacket);

            byte[] dataBytes = createWriteData(DATA_PACKET_MAX_SIZE - 1, blockNum, data);
            socket.send(new DatagramPacket(dataBytes, dataBytes.length, recPacket.getAddress(), recPacket.getPort()));

            recPacket = new DatagramPacket(new byte[MAX_PACKET_SIZE], MAX_PACKET_SIZE);
            socket.receive(recPacket);
            printPacket(recPacket);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {}

            // Inject two more "retransmissions" to test out the Server
            socket.send(new DatagramPacket(dataBytes, dataBytes.length, recPacket.getAddress(), recPacket.getPort()));
            socket.receive(recPacket);
            printPacket(recPacket);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {}

            socket.send(new DatagramPacket(dataBytes, dataBytes.length, recPacket.getAddress(), recPacket.getPort()));
            socket.receive(recPacket);
            printPacket(recPacket);

        } catch (SocketException se) {
            se.printStackTrace();
            System.exit(-1);
        } catch (UnknownHostException uhe) {
            uhe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    static byte[] createWriteRequest(String filename) {
        byte[] ret = new byte[2 + filename.length() + 10];
        ret[1] = (byte)WRQ_OPCODE;
        for (int i = 0; i < filename.length(); i++) {
            ret[i+2] = (byte)filename.charAt(i);
        }
        String mode = "netascii";
        for (int i = 0; i < mode.length(); i++) {
            ret[i+3+filename.length()] = (byte)mode.charAt(i);
        }
        return ret;
    }

    static byte[] createWriteData(int length, short blockNum, String data) {
        byte[] dataBytes = new byte[length];
        dataBytes[1] = (byte)DATA_OPCODE;
        dataBytes[2] = (byte)((blockNum >> 8) & 0xff);
        dataBytes[3] = (byte)(blockNum & 0xff);
        System.arraycopy(data.getBytes(), 0, dataBytes, 4, data.length());
        return dataBytes;
    }

    static void printPacket(DatagramPacket packet) {
        System.out.println("received packet from port " + packet.getPort() + " and addr " + packet.getAddress());
        byte[] data = packet.getData();
        for (int i = 0; i < packet.getLength(); i++) {
            System.out.println(data[i] + "    " + (char)(data[i]));
        }
    }
}
