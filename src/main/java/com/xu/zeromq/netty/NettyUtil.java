package com.xu.zeromq.netty;

import com.xu.zeromq.core.MessageSystemConfig;
import io.netty.channel.Channel;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;
import com.google.common.base.Preconditions;

public class NettyUtil {

    public static final String osName = System.getProperty("os.name");

    private static boolean isLinux = false;
    private static boolean isWindows = false;

    static {
        if (osName != null && osName.toLowerCase().contains("linux")) {
            isLinux = true;
        }

        if (osName != null && osName.toLowerCase().contains("windows")) {
            isWindows = true;
        }
    }

    public static boolean isLinux() {
        return isLinux;
    }

    public static boolean isWindows() {
        return isWindows;
    }

    public static SelectorProvider getNioSelectorProvider() throws IOException {
        Selector result = null;
        if (isLinux()) {
            try {
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        final Method method = providerClazz.getMethod("provider");
                        if (method != null) {
                            final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (final Exception e) {
                    }
                }
            } catch (final Exception e) {
            }
        }

        if (result == null) {
            result = Selector.open();
        }

        return result.provider();
    }

    public static String getLocalIpAddress() {
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            ArrayList<String> ipv4Result = new ArrayList<String>();
            ArrayList<String> ipv6Result = new ArrayList<String>();
            while (enumeration.hasMoreElements()) {
                final NetworkInterface networkInterface = enumeration.nextElement();
                final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
                while (en.hasMoreElements()) {
                    final InetAddress address = en.nextElement();
                    if (!address.isLoopbackAddress()) {
                        if (address instanceof Inet6Address) {
                            ipv6Result.add(formatHostAddress(address));
                        } else {
                            ipv4Result.add(formatHostAddress(address));
                        }
                    }
                }
            }

            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
                        continue;
                    }
                    return ip;
                }

                return ipv4Result.get(ipv4Result.size() - 1);
            } else if (!ipv6Result.isEmpty()) {
                return ipv6Result.get(0);
            }

            final InetAddress localHost = InetAddress.getLocalHost();
            return formatHostAddress(localHost);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String formatHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        String[] addrs = addr.split(MessageSystemConfig.IpV4AddressDelimiter);
        return new InetSocketAddress(addrs[0], Integer.parseInt(addrs[1]));
    }

    public static String socketAddress2String(final SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getHostAddress());
        sb.append(":");
        sb.append(inetSocketAddress.getPort());
        return sb.toString();
    }

    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            sc.socket().setSoLinger(false, -1);
            sc.socket().setTcpNoDelay(true);
            sc.socket().setReceiveBufferSize(1024 * 64);
            sc.socket().setSendBufferSize(1024 * 64);
            sc.socket().connect(remote, timeoutMillis);
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }

    public static boolean validateChannel(Channel channel) {
        // 检查 channel 对象是否为 null
        Preconditions.checkNotNull(channel, "channel cannot be null");
        // 确认 channel 是否是 active，是否开启 open，以及是否可以写入
        return channel.isActive() && channel.isOpen() && channel.isWritable();
    }
}
