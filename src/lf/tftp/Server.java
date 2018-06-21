package lf.tftp;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server extends Thread {
    private static final int TFTP_PORT = 69;
    private static final int MAX_REQUEST_SIZE = 512;
    private static final int TIMEOUT = 5000;    // in ms

    private static short RRQ_OPCODE   = 1;
    private static short WRQ_OPCODE   = 2;
    private static short DATA_OPCODE  = 3;
    private static short ACK_OPCODE   = 4;
    private static short ERROR_OPCODE = 5;

    private ExecutorService pool;
    private DatagramSocket socket;
    private byte[] buffer;

    private static class TFTPPacket {
        enum Operation { READ, WRITE, DATA, ACK, ERROR }
        protected Operation operation;
        protected InetAddress sourceAddr;
        protected int sourcePort;

        TFTPPacket(InetAddress sourceAddr, int sourcePort) {
            this.sourceAddr = sourceAddr;
            this.sourcePort = sourcePort;
        }

        Operation getOperation() { return this.operation; }
        InetAddress getSourceAddr() { return this.sourceAddr; }
        int getSourcePort() { return this.sourcePort; }
    }

    private static class RequestPacket extends TFTPPacket {
        private static final int FILENAME_OFFSET = 2;

        private String filename;
        private String mode;

        RequestPacket(InetAddress sourceAddr, int sourcePort, byte[] data, int length) {
            super(sourceAddr, sourcePort);
            short opcode = (short)((data[0] << 8) | data[1]);
            if (opcode == WRQ_OPCODE) {
                this.operation = Operation.WRITE;
            } else {
                throw new IllegalArgumentException("Only Write Operations are supported! Received: " + opcode);
            }

            this.filename = parseNullTerminatedString(data, FILENAME_OFFSET);
            this.mode = parseNullTerminatedString(data, FILENAME_OFFSET + this.filename.length() + 2);
        }

        private String parseNullTerminatedString(byte[] buf, int start) {
            int index = start;
            while (buf[index] != (byte)0) { index++; }
            byte[] stringBytes = new byte[index - start];
            System.arraycopy(buf, start, stringBytes, 0, index - start);
            return new String(stringBytes);
        }

        String getFilename() { return this.filename; }

        void debugPacket(byte[] data) {
            for (int i = 0; i < data.length; i++) {
                System.out.println((char)data[i]);
            }
        }
    }

    private static class RequestHandler implements Runnable {
        private static final int DATA_PACKET_MAX_BYTES_COUNT = 512;
        private static final int DATA_PACKET_BYTES_OFFSET = 4;
        private static final int DATA_PACKET_MAX_SIZE = DATA_PACKET_MAX_BYTES_COUNT + DATA_PACKET_BYTES_OFFSET;

        private static final int PACKET_OPCODE_OFFSET = 0;
        private static final int PACKET_BLOCKNUM_OFFSET = 2;

        private static final int ACK_PACKET_SIZE = 4;

        private static final int ERROR_PACKAGE_BASE_SIZE = 5;    // string size additional
        private static final int ERROR_PACKAGE_ERROR_CODE_OFFSET = 2;
        private static final int ERROR_PACKAGE_MESSAGE_OFFSET = 4;
        private static final short ERROR_UNDEFINED = (short)0;
        private static final short ERROR_ACCESS_VIOLATION = (short)2;
        private static final short ERROR_ILLEGAL_TFTP_OPERATION = (short)4;
        private static final short ERROR_UNKNOWN_TID = (short)5;

        private static final int MAX_TIMEOUTS = 5;    // error after this many consecutive timeouts

        private InetAddress sourceAddr;
        private int sourcePort;
        private FileOutputStream fos;
        private String filename;

        RequestHandler(RequestPacket reqPacket) {
            this.sourceAddr = reqPacket.getSourceAddr();
            this.sourcePort = reqPacket.getSourcePort();
            this.filename = reqPacket.getFilename();
        }

        public void run() {
            try (DatagramSocket socket = new DatagramSocket()) {
                try {
                    this.fos = new FileOutputStream(this.filename);
                } catch (FileNotFoundException fnfe) {
                    // Categorizing all file access issues as access violation
                    byte[] errorMsgData = createErrorData(ERROR_ACCESS_VIOLATION, null);
                    socket.send(new DatagramPacket(errorMsgData, errorMsgData.length, sourceAddr, sourcePort));
                    System.out.println("Could not open requested file, exiting");
                    return;
                }

                boolean connected = true;
                int timeoutCount = 0;
                socket.setSoTimeout(TIMEOUT);

                short blockNum = (short)0;
                DatagramPacket ackPacket = new DatagramPacket(createAckData(blockNum), ACK_PACKET_SIZE, sourceAddr, sourcePort);
                socket.send(ackPacket);

                DatagramPacket packet = new DatagramPacket(new byte[DATA_PACKET_MAX_SIZE], DATA_PACKET_MAX_SIZE);
                while (connected) {
                    try {
                        socket.receive(packet);
                    } catch (SocketTimeoutException ste) {
                        timeoutCount++;
                        if (timeoutCount > MAX_TIMEOUTS) break;
                        socket.send(ackPacket);   // retransmit the last ackPacket if timed out
                        System.out.println("Just re-sent last ack due to timeout");
                        continue;
                    }
                    timeoutCount = 0;    // reset if a valid packet was received in time
                    byte[] data = packet.getData();

                    // Received packet from wrong source TID: respond to that TID with error packet
                    int recPort = packet.getPort();
                    if (recPort != this.sourcePort) {
                        byte[] errorMsgData = createErrorData(ERROR_UNKNOWN_TID, null);
                        socket.send(new DatagramPacket(errorMsgData, errorMsgData.length, sourceAddr, recPort));
                        System.out.println("Just sent response to client for bad TID");
                        continue;
                    }

                    // Check opcode; if not Data, send error
                    short recOpcode = bytesToShort(data, PACKET_OPCODE_OFFSET);
                    if (recOpcode != DATA_OPCODE) {
                        byte[] errorMsgData = createErrorData(ERROR_ILLEGAL_TFTP_OPERATION, null);
                        socket.send(new DatagramPacket(errorMsgData, errorMsgData.length, sourceAddr, sourcePort));
                        System.out.println("Terminating due to bad opcode");
                        break;    // terminate immediately
                    }

                    // Check blocknum, expected to be 1 higher than last one
                    // If same as last one, retransmit; anything else is error (considered improperly formed packet)
                    short recBlockNum = bytesToShort(data, PACKET_BLOCKNUM_OFFSET);
                    if (recBlockNum != blockNum + 1) {
                        if (recBlockNum == blockNum) {
                            socket.send(ackPacket);    // sender retransmitted last packet; do nothing with data and retransmit last ack
                            System.out.println("Just re-sent last ack due to repeating blockNum from client");
                            continue;
                        } else {
                            byte[] errorMsgData = createErrorData(ERROR_UNDEFINED, "Bad BlockNum Received");
                            socket.send(new DatagramPacket(errorMsgData, errorMsgData.length, sourceAddr, sourcePort));
                            System.out.println("Just received bad blockNum, terminating");
                            break;    // terminate immediately
                        }
                    }

                    if (packet.getLength() < DATA_PACKET_MAX_SIZE) {    // termination condition
                        ackPacket = new DatagramPacket(createAckData(++blockNum), ACK_PACKET_SIZE, sourceAddr, sourcePort);
                        socket.send(ackPacket);
                        System.out.println("Just sent final ack");

                        while (true) {
                            // Dally a bit to possibly re-send ACK
                            try {
                                socket.receive(packet);
                            } catch (SocketTimeoutException ste) {
                                System.out.println("Did not receive any more, terminating");
                                // Did not receive any retransmissions, assume last ACK was delivered properly, just exit
                                connected = false;
                                break;
                            }

                            // Did receive a retransmission; resend ACK
                            data = packet.getData();
                            recOpcode = bytesToShort(data, PACKET_OPCODE_OFFSET);
                            recBlockNum = bytesToShort(data, PACKET_BLOCKNUM_OFFSET);

                            if (recOpcode != DATA_OPCODE || recBlockNum != blockNum) {
                                connected = false;
                                break;    // error, just close connection
                            }
                            else socket.send(ackPacket);
                            System.out.println("Just re-sent final ack due to retransmission of last data packet");
                        }
                    } else {
                        this.fos.write(data, DATA_PACKET_BYTES_OFFSET, DATA_PACKET_MAX_BYTES_COUNT);
                        ackPacket = new DatagramPacket(createAckData(++blockNum), ACK_PACKET_SIZE, sourceAddr, sourcePort);
                        socket.send(ackPacket);
                        System.out.println("Wrote data, sent ack, new blockNum " + blockNum);
                    }
                }
            } catch (SocketException se) {
                throw new RuntimeException("Can't open socket for response: " + se.getMessage());
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getCause());
            }
            System.out.println("connection terminated");
        }

        byte[] createAckData(short blockNum) {
            return new byte[]{(byte)0, (byte)4, (byte)((blockNum >> 8) & 0xff), (byte)(blockNum & 0xff)}; // big endian
        }

        short bytesToShort(byte[] data, int start) {
            return (short)((data[start] << 8) | data[start+1]);
        }

        void shortToBytes(byte[] data, int start, short num) {
            data[start] = (byte)((num >> 8) & 0xff);
            data[start + 1] = (byte)(num & 0xff);
        }

        byte[] createErrorData(short errorCode, String msg) {
            int length = errorCode != ERROR_UNDEFINED ? ERROR_PACKAGE_BASE_SIZE : ERROR_PACKAGE_BASE_SIZE + msg.length();
            byte[] data = new byte[length];
            shortToBytes(data, PACKET_OPCODE_OFFSET, ERROR_OPCODE);
            shortToBytes(data, ERROR_PACKAGE_ERROR_CODE_OFFSET, errorCode);
            if (errorCode == ERROR_UNDEFINED) {
                System.arraycopy(msg.getBytes(), 0, data, ERROR_PACKAGE_MESSAGE_OFFSET, msg.length());
            }
            return data;
        }
    }


    public Server(int numThreads) {
        this.pool = Executors.newFixedThreadPool(numThreads);
        try {
            this.socket = new DatagramSocket(TFTP_PORT, InetAddress.getLocalHost());
        } catch (SocketException se) {
            throw new RuntimeException(se.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        this.buffer = new byte[MAX_REQUEST_SIZE];
    }

    public void run() {
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }

            InetAddress addr = packet.getAddress();
            int port = packet.getPort();
            byte[] data = packet.getData();
            RequestPacket reqPacket = new RequestPacket(addr, port, data, packet.getLength());
            this.pool.execute(new RequestHandler(reqPacket));
        }
    }

    public static void main(String[] args) {
        Server server = new Server(10);
        server.start();
    }
}
