/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.common.utils;

import static java.util.Collections.emptyList;

import org.apache.dolphinscheduler.common.constants.Constants;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.conn.util.InetAddressUtils;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

/**
 * NetUtils
 */
@Slf4j
public class NetUtils {

    private static final String NETWORK_PRIORITY_DEFAULT = "default";
    private static final String NETWORK_PRIORITY_INNER = "inner";
    private static final String NETWORK_PRIORITY_OUTER = "outer";
    private static InetAddress LOCAL_ADDRESS = null;
    private static volatile String HOST_ADDRESS;

    private NetUtils() {
        throw new UnsupportedOperationException("Construct NetUtils");
    }

    /**
     * get addr like host:port
     * @return addr
     */
    public static String getAddr(String host, int port) {
        return String.format("%s:%d", host, port);
    }

    /**
     * get addr like host:port
     * @return addr
     */
    public static String getAddr(int port) {
        return getAddr(getHost(), port);
    }

    /**
     * get host
     * @return host
     */
    public static String getHost(InetAddress inetAddress) {
        if (inetAddress != null) {
            if (KubernetesUtils.isKubernetesMode()) {
                String canonicalHost = inetAddress.getCanonicalHostName();
                String[] items = canonicalHost.split("\\.");
                if (items.length == 6 && "svc".equals(items[3])) {
                    return String.format("%s.%s", items[0], items[1]);
                }
                return canonicalHost;
            }
            return inetAddress.getHostAddress();
        }
        return null;
    }

    public static String getHost() {
        if (HOST_ADDRESS != null) {
            return HOST_ADDRESS;
        }

        InetAddress address = getLocalAddress();
        if (address != null) {
            HOST_ADDRESS = getHost(address);
            return HOST_ADDRESS;
        }
        return KubernetesUtils.isKubernetesMode() ? "localhost" : "127.0.0.1";
    }

    private static InetAddress getLocalAddress() {
        if (null != LOCAL_ADDRESS) {
            return LOCAL_ADDRESS;
        }
        return getLocalAddress0();
    }

    /**
     * Find first valid IP from local network card
     *
     * @return first valid local IP
     */
    private static synchronized InetAddress getLocalAddress0() {
        if (null != LOCAL_ADDRESS) {
            return LOCAL_ADDRESS;
        }

        InetAddress localAddress = null;
        try {
            Optional<NetworkInterface> networkInterface = findNetworkInterface();
            if (networkInterface.isPresent()) {
                Enumeration<InetAddress> addresses = networkInterface.get().getInetAddresses();
                while (addresses.hasMoreElements()) {
                    Optional<InetAddress> addressOp = toValidAddress(addresses.nextElement());
                    if (addressOp.isPresent()) {
                        try {
                            if (addressOp.get().isReachable(200)) {
                                LOCAL_ADDRESS = addressOp.get();
                                return LOCAL_ADDRESS;
                            }
                        } catch (IOException e) {
                            log.warn("test address id reachable io exception", e);
                        }
                    }
                }

            }
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            log.warn("InetAddress get LocalHost exception", e);
        }
        Optional<InetAddress> addressOp = toValidAddress(localAddress);
        if (addressOp.isPresent()) {
            LOCAL_ADDRESS = addressOp.get();
        }
        return LOCAL_ADDRESS;
    }

    private static Optional<InetAddress> toValidAddress(InetAddress address) {
        if (address instanceof Inet6Address) {
            Inet6Address v6Address = (Inet6Address) address;
            if (isPreferIPV6Address()) {
                InetAddress inetAddress = normalizeV6Address(v6Address);
                log.debug("The host prefer ipv6 address, will use ipv6 address: {} directly", inetAddress);
                return Optional.ofNullable(inetAddress);
            }
        }
        if (isValidV4Address(address)) {
            return Optional.of(address);
        }
        log.warn("The address of the host is invalid, address: {}", address);
        return Optional.empty();
    }

    private static InetAddress normalizeV6Address(Inet6Address address) {
        String addr = address.getHostAddress();
        int i = addr.lastIndexOf('%');
        if (i > 0) {
            try {
                return InetAddress.getByName(addr.substring(0, i) + '%' + address.getScopeId());
            } catch (UnknownHostException e) {
                log.debug("Unknown IPV6 address: ", e);
            }
        }
        return address;
    }

    public static boolean isValidV4Address(InetAddress address) {

        if (address == null || address.isLoopbackAddress()) {
            return false;
        }
        String name = address.getHostAddress();
        return (name != null
                && InetAddressUtils.isIPv4Address(name)
                && !address.isAnyLocalAddress()
                && !address.isLoopbackAddress());
    }

    /**
     * Check if an ipv6 address
     *
     * @return true if it is reachable
     */
    private static boolean isPreferIPV6Address() {
        return Boolean.getBoolean("java.net.preferIPv6Addresses");
    }

    /**
     * Get the suitable {@link NetworkInterface}
     *
     * @return If no {@link NetworkInterface} is available , return <code>null</code>
     */
    private static Optional<NetworkInterface> findNetworkInterface() {

        List<NetworkInterface> validNetworkInterfaces = emptyList();

        try {
            validNetworkInterfaces = getValidNetworkInterfaces();
        } catch (SocketException e) {
            log.warn("ValidNetworkInterfaces exception", e);
        }
        if (CollectionUtils.isEmpty(validNetworkInterfaces)) {
            log.warn("ValidNetworkInterfaces is empty");
            return Optional.empty();
        }

        // Try to specify config NetWork Interface
        Optional<NetworkInterface> specifyNetworkInterface =
                validNetworkInterfaces.stream().filter(NetUtils::isSpecifyNetworkInterface).findFirst();
        if (specifyNetworkInterface.isPresent()) {
            log.info("Use specified NetworkInterface: {}", specifyNetworkInterface.get());
            return specifyNetworkInterface;
        }

        return findAddress(validNetworkInterfaces);
    }

    /**
     * Get the valid {@link NetworkInterface network interfaces}
     *
     * @throws SocketException SocketException if an I/O error occurs.
     */
    private static List<NetworkInterface> getValidNetworkInterfaces() throws SocketException {
        List<NetworkInterface> validNetworkInterfaces = new LinkedList<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            // ignore
            if (ignoreNetworkInterface(networkInterface)) {
                log.debug("Info NetworkInterface: {}", networkInterface);
                continue;
            }
            log.info("Found valid NetworkInterface: {}", networkInterface);
            validNetworkInterfaces.add(networkInterface);
        }
        return validNetworkInterfaces;
    }

    /**
     * @param networkInterface {@link NetworkInterface}
     * @return if the specified {@link NetworkInterface} should be ignored, return <code>true</code>
     * @throws SocketException SocketException if an I/O error occurs.
     */
    public static boolean ignoreNetworkInterface(NetworkInterface networkInterface) throws SocketException {
        return networkInterface == null
                || networkInterface.isLoopback()
                || networkInterface.isVirtual()
                || !networkInterface.isUp();
    }

    private static boolean isSpecifyNetworkInterface(NetworkInterface networkInterface) {
        String preferredNetworkInterface =
                PropertyUtils.getString(Constants.DOLPHIN_SCHEDULER_NETWORK_INTERFACE_PREFERRED,
                        System.getProperty(Constants.DOLPHIN_SCHEDULER_NETWORK_INTERFACE_PREFERRED));
        return Objects.equals(networkInterface.getDisplayName(), preferredNetworkInterface);
    }

    private static Optional<NetworkInterface> findAddress(List<NetworkInterface> validNetworkInterfaces) {
        if (CollectionUtils.isEmpty(validNetworkInterfaces)) {
            return Optional.empty();
        }
        String networkPriority = PropertyUtils.getString(Constants.DOLPHIN_SCHEDULER_NETWORK_PRIORITY_STRATEGY,
                NETWORK_PRIORITY_DEFAULT);
        switch (networkPriority) {
            case NETWORK_PRIORITY_DEFAULT:
                log.debug("Use default NetworkInterface acquisition policy");
                return findAddressByDefaultPolicy(validNetworkInterfaces);
            case NETWORK_PRIORITY_INNER:
                log.debug("Use inner NetworkInterface acquisition policy");
                return findInnerAddress(validNetworkInterfaces);
            case NETWORK_PRIORITY_OUTER:
                log.debug("Use outer NetworkInterface acquisition policy");
                return findOuterAddress(validNetworkInterfaces);
            default:
                log.error("There is no matching network card acquisition policy!");
                return Optional.empty();
        }
    }

    private static Optional<NetworkInterface> findAddressByDefaultPolicy(List<NetworkInterface> validNetworkInterfaces) {
        Optional<NetworkInterface> innerAddress = findInnerAddress(validNetworkInterfaces);
        if (innerAddress.isPresent()) {
            log.debug("Found inner NetworkInterface: {}", innerAddress.get());
            return innerAddress;
        }
        Optional<NetworkInterface> outerAddress = findOuterAddress(validNetworkInterfaces);
        if (outerAddress.isPresent()) {
            log.debug("Found outer NetworkInterface: {}", outerAddress.get());
            return outerAddress;
        }
        return Optional.empty();
    }

    /**
     * Get the Intranet IP
     *
     * @return If no {@link NetworkInterface} is available , return <code>null</code>
     */
    private static Optional<NetworkInterface> findInnerAddress(List<NetworkInterface> validNetworkInterfaces) {
        if (CollectionUtils.isEmpty(validNetworkInterfaces)) {
            return Optional.empty();
        }

        for (NetworkInterface ni : validNetworkInterfaces) {
            Enumeration<InetAddress> address = ni.getInetAddresses();
            while (address.hasMoreElements()) {
                InetAddress ip = address.nextElement();
                if (ip.isSiteLocalAddress()
                        && !ip.isLoopbackAddress()) {
                    return Optional.of(ni);
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<NetworkInterface> findOuterAddress(List<NetworkInterface> validNetworkInterfaces) {
        if (CollectionUtils.isEmpty(validNetworkInterfaces)) {
            return Optional.empty();
        }
        for (NetworkInterface ni : validNetworkInterfaces) {
            Enumeration<InetAddress> address = ni.getInetAddresses();
            while (address.hasMoreElements()) {
                InetAddress ip = address.nextElement();
                if (!ip.isSiteLocalAddress()
                        && !ip.isLoopbackAddress()) {
                    return Optional.of(ni);
                }
            }
        }
        return Optional.empty();
    }

}
