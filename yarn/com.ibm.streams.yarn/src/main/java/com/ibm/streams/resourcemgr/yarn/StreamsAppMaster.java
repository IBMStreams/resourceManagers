//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streams.resourcemgr.yarn;

import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.messages.Message;
import com.ibm.streams.messages.StreamsRuntimeMessagesKey.Key;
import com.ibm.streams.resourcemgr.AllocateInfo;
import com.ibm.streams.resourcemgr.AllocateInfo.AllocateType;
import com.ibm.streams.resourcemgr.AllocateMasterInfo;
import com.ibm.streams.resourcemgr.ClientInfo;
import com.ibm.streams.resourcemgr.ResourceDescriptor;
import com.ibm.streams.resourcemgr.ResourceDescriptor.ResourceKind;
import com.ibm.streams.resourcemgr.ResourceDescriptorState;
import com.ibm.streams.resourcemgr.ResourceDescriptorState.AllocateState;
import com.ibm.streams.resourcemgr.ResourceException;
import com.ibm.streams.resourcemgr.ResourceManagerAdapter;
import com.ibm.streams.resourcemgr.ResourceManagerException;
import com.ibm.streams.resourcemgr.ResourceTagException;
import com.ibm.streams.resourcemgr.ResourceTags;
import com.ibm.streams.resourcemgr.ResourceTags.TagDefinitionType;
import com.ibm.streams.resourcemgr.exception.ResourceTagMessageException;

public class StreamsAppMaster extends ResourceManagerAdapter {

    static final int APP_MASTER_DUMMY_PORT = 9090;

    private static final Logger LOG = LoggerFactory.getLogger(StreamsAppMaster.class);

    AMRMClientImpl<ContainerRequest> streamsAMRM;
    private YarnConfiguration yarnConf;
    NMClientImpl nmClient;

    private Map<String, ContainerWrapper> assignedContainers = new HashMap<String, ContainerWrapper>();
    private List<ContainerRequestWrapper> newRequests = new ArrayList<ContainerRequestWrapper>();
    private List<ContainerRequestWrapper> pendingRequests = new ArrayList<ContainerRequestWrapper>();
    private Map<String, ContainerRequestWrapper> allRequestsMap = new HashMap<String, ContainerRequestWrapper>();
    private BlockingQueue<ANotification> notificationQueue = new LinkedBlockingQueue<ANotification>();

    private String installPath = null, homeDir = null;
    ClientInfo clientInfo = null;
    private int currentPriority = 0;
    private boolean shutdown = false, deployStreams = false, isInit = false;
    private ExecutorService executorService = null;
    private Map<String, String> envs = System.getenv();
    private Properties props = null;

    private Set<String> toNotifyList = new HashSet<String>();

    public StreamsAppMaster(String [] args) throws Exception {

        yarnConf = new YarnConfiguration();
        streamsAMRM = new AMRMClientImpl<ContainerRequest>();
        nmClient = (NMClientImpl)NMClient.createNMClient();
        executorService = Executors.newFixedThreadPool(2);
        props = new Properties();
        props.load(new FileReader(StreamsYarnConstants.AM_PROPERTIES_FILE));
        LOG.info("AM properties: " + props.toString());
        LOG.debug("Environment: " + envs);

        for (int index = 0; index < args.length; ++index) {
            String arg = args[index];

            if (arg.equals(StreamsYarnConstants.INSTALL_PATH_ARG) && index + 1 < args.length) {
                installPath = args[++index];
            } else if (arg.equals(StreamsYarnConstants.HOME_DIR_ARG) && index + 1 < args.length) {
                homeDir = args[++index];
            } else if (arg.equals(StreamsYarnConstants.DEPLOY_ARG)) {
                deployStreams = true;
                ++index;//for the path to streams which is not relevant here
            }
        }
        //path will be set later via pattern replacement
        if (deployStreams)
            installPath = "%STREAMS_INSTALL%";

    }

    @Override
    public void initialize() throws ResourceManagerException {
        try {
            nmClient.init(yarnConf);
            nmClient.start();
            streamsAMRM.init(yarnConf);
            streamsAMRM.start();

            LOG.info("Registering StreamsAM with RM");
            RegisterApplicationMasterResponse response = streamsAMRM.registerApplicationMaster(Utils.getHostName(),
                    APP_MASTER_DUMMY_PORT, null);

            LOG.info(response.toString());

            // start tasks
            executorService.submit(rmTask);
            executorService.submit(notifTask);

            isInit = true;
            LOG.info("Main Loop Started");
        } catch (Exception e) {
            throw new ResourceManagerException(e);
        }
    }

    public boolean deployStreams() {
        return deployStreams;
    }

    @Override
    public void close() {
        LOG.info("Close");
        if (!isInit)
            return;
        try {
            shutdown = true;
            executorService.shutdown();
            // tell RM that we're done	
            LOG.info("Unregistering StreamsAM with RM");
            streamsAMRM.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Done", null);
            nmClient.stop();
            streamsAMRM.stop();

        } catch (Exception e) {
            LOG.error("", e);
        }

    }

    static private int getWaitSecs(AllocateType rType, Properties props) {

        switch (rType) {
            case SYNCHRONOUS:
                return Utils.getIntProperty(props, StreamsYarnConstants.PROPS_WAIT_SYNC);
            case ASYNCHRONOUS:
                return Utils.getIntProperty(props, StreamsYarnConstants.PROPS_WAIT_ASYNC); //not much wait
            case FLEXIBLE:
                // at least wait for one timer cycle
                return Utils.getIntProperty(props, StreamsYarnConstants.PROPS_WAIT_HEARTBEAT)
                        + Utils.getIntProperty(props, StreamsYarnConstants.PROPS_WAIT_FLEXIBLE);
            default:
                throw new RuntimeException("Unhandled type: " + rType);
        }
    }

    private List<ResourceDescriptorState> waitForAllocation(List<ContainerRequestWrapper> requests, AllocateType rType) {
        int waitTimeSecs = getWaitSecs(rType, props);
        LOG.info("Waiting for resources to be allocated and running, maxTime: "
                + (waitTimeSecs < 0 ? "unbounded" : waitTimeSecs));
        long endTime = System.currentTimeMillis() + (waitTimeSecs * 1000);

        while (waitTimeSecs < 0 || System.currentTimeMillis() < endTime) {
            int allocCount = 0;
            synchronized (this) {
                for (ContainerRequestWrapper crw : requests) {
                    if (crw.isRunning()) {
                        allocCount++;
                    }
                }
                LOG.debug("Allocated Count: " + allocCount);
                if (allocCount == requests.size()) {
                    break;
                }
            }
            Utils.sleepABit(StreamsYarnConstants.SLEEP_UNIT_MILLIS);
        }
        synchronized (this) {
            List<ResourceDescriptorState> states = new ArrayList<ResourceDescriptorState>();
            for (ContainerRequestWrapper crw : requests) {
                states.add(crw.getDescriptorState());
                toNotifyList.add(crw.getDescriptor().getNativeResourceId());
            }
            LOG.info("Returning state: " + states);
            return states;
        }

    }

    @Override
    public ResourceDescriptor allocateMasterResource(ClientInfo clientInfo, AllocateMasterInfo info)
            throws ResourceTagException, ResourceManagerException {
        LOG.info("Allocate Master Request: " + info);

        List<ResourceDescriptorState> lst = allocateResources(clientInfo, true, 1, info.getTags(),
                AllocateType.SYNCHRONOUS);
        if (lst.size() == 0)
            throw new ResourceManagerException("Could not allocate master resource");
        return lst.get(0).getDescriptor();
    }

    @Override
    public Collection<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, AllocateInfo request)
            throws ResourceTagException, ResourceManagerException {
        LOG.info("Allocate Request: " + request);

        return allocateResources(clientInfo, false, request.getCount(), request.getTags(), request.getType());
    }

    private List<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, boolean isMaster, int count,
            ResourceTags tags, AllocateType rType) throws ResourceManagerException, ResourceTagException {

        List<ContainerRequestWrapper> requestWrappers = new ArrayList<ContainerRequestWrapper>();
        synchronized (this) {
            this.clientInfo = clientInfo;

            for (int i = 0; i < count; i++) {
                if (currentPriority == Integer.MAX_VALUE)
                    currentPriority = 0; // check for overflow
                ContainerRequestWrapper crw = new ContainerRequestWrapper(Utils.generateNextId("container"),
                        clientInfo.getDomainId(), clientInfo.getZkConnect(), currentPriority++);

                crw.setMaster(isMaster);
                if (tags != null) {
                    convertTags(tags, crw); //set mem/cpu by tags
                    crw.getTags().addAll(tags.getNames());
                }
                requestWrappers.add(crw);
            }
            //once all requests are validated
            for (ContainerRequestWrapper crw : requestWrappers) {
                LOG.info("Queuing request: " + crw);
                newRequests.add(crw);
                allRequestsMap.put(crw.getId(), crw);
            }
        }
        LOG.info("Waiting for resources to be allocated");
        return waitForAllocation(requestWrappers, rType);
    }

    private void requestResource(ContainerRequestWrapper containerRequestWrapper) throws StreamsYarnException {
        LOG.info("Requesting container: " + containerRequestWrapper);
        try {
            ContainerRequest cr = containerRequestWrapper.getCreateResourceRequest();
            LOG.info("Created container request: " + cr);
            streamsAMRM.addContainerRequest(cr);
        } catch (UnknownHostException e) {
            throw new StreamsYarnException(e.getMessage());
        }
        LOG.info("Done requesting container");
    }

    @Override
    public synchronized void releaseResources(ClientInfo clientInfo, Locale locale) throws ResourceManagerException {
        LOG.info("releaseClientResources");
        this.clientInfo = clientInfo;

        for (ContainerRequestWrapper crw : allRequestsMap.values()) {
            crw.getAllocatedContainer().stop();
        }
    }

    @Override
    public synchronized void releaseResources(ClientInfo clientInfo, Collection<ResourceDescriptor> resourceDescriptors, Locale locale)
            throws ResourceManagerException {
        LOG.info("releaseResources: " + resourceDescriptors);
        this.clientInfo = clientInfo;

        for (ResourceDescriptor rd : resourceDescriptors) {
            try {
                if (!allRequestsMap.containsKey(rd.getNativeResourceId()))
                    throw new ResourceManagerException("No such container: " + rd.getNativeResourceId());
                ContainerRequestWrapper crw = allRequestsMap.get(rd.getNativeResourceId());
                crw.getAllocatedContainer().stop();
            } catch (Exception e) {
                LOG.error("Error stopping container: " + rd.getNativeResourceId(), e);
            }
        }
    }

    @Override
    public synchronized void cancelPendingResources(ClientInfo clientInfo,
            Collection<ResourceDescriptor> resourceDescriptors, Locale locale) throws ResourceManagerException {
        LOG.info("cancelPendingResources");

        this.clientInfo = clientInfo;
        for (ResourceDescriptor rd : resourceDescriptors) {
            try {
                if (!allRequestsMap.containsKey(rd.getNativeResourceId()))
                    throw new ResourceManagerException("No such container: " + rd.getNativeResourceId());
                ContainerRequestWrapper crw = allRequestsMap.get(rd.getNativeResourceId());
                LOG.info("Cancelling Resource: " + crw);
                crw.cancel();
            } catch (Exception e) {
                LOG.error("Error cancelling container: " + rd.getNativeResourceId(), e);
            }
        }
    }

    private void releaseContainer(ContainerWrapper cw) {
        cw.release();
        assignedContainers.remove(cw.getContainer().getId().toString());
        allRequestsMap.remove(cw.getId());
    }

    private Runnable rmTask = new Runnable() {
        @Override
        public void run() {
            LOG.info("Launched RM connection thread");
            try {
                while (!shutdown) {

                    boolean ret = runAllocate();
                    int wait = Utils.getIntProperty(props, StreamsYarnConstants.PROPS_WAIT_HEARTBEAT);
                    if (ret)
                        wait = 1;
                    long begin = System.currentTimeMillis();
                    while (!shutdown && System.currentTimeMillis() - begin < wait * 1000)
                        Utils.sleepABit(StreamsYarnConstants.SLEEP_UNIT_MILLIS);
                }
            } catch (Exception e) {
                LOG.error("StreamsAM Exception: ", e);
            }
            LOG.info("RM connection thread exiting");
        }
    };

    private Runnable notifTask = new Runnable() {
        @Override
        public void run() {
            LOG.info("Launched notification thread");
            try {
                while (!shutdown) {
                    notificationQueue.take().notifyStreams();
                }
            } catch (Exception e) {
                LOG.error("StreamsAM Exception: ", e);
            }
            LOG.info("Notification thread exiting");
        }
    };

    private ContainerRequestWrapper getMatchingRequest(ContainerWrapper containerWrapper) throws StreamsYarnException {
        int count = pendingRequests.size();
        LOG.info("Checking request for container: " + containerWrapper);
        for (int i = 0; i < count; i++) {
            ContainerRequestWrapper crw = pendingRequests.get(i);
            LOG.info("Comparing Request: " + crw);
            if (crw.matches(containerWrapper)) {
                pendingRequests.remove(i);
                LOG.info("Found Matching Request: " + crw);
                return crw;
            }
        }
        throw new StreamsYarnException("No matching request found for container: " + containerWrapper);
    }

    void addLocalResources(Map<String, LocalResource> localResources, LocalResourceType rType, String key,
            String resName) throws URISyntaxException {
        if (!envs.containsKey(resName))
            return;
        LOG.debug("Adding resource: " + envs.get(resName));
        LocalResource lResource = Records.newRecord(LocalResource.class);
        lResource.setType(rType);
        lResource.setVisibility(LocalResourceVisibility.APPLICATION);
        lResource.setResource(ConverterUtils.getYarnUrlFromURI(new URI(envs.get(resName))));
        lResource.setTimestamp(Long.parseLong(envs.get(resName + "_TIME")));
        lResource.setSize(Long.parseLong(envs.get(resName + "_LEN")));
        localResources.put(key, lResource);
    }

    private void containerAssigned(ContainerWrapper containerWrapper) {
        LOG.info("Launching Container: " + containerWrapper);
        try {
            ContainerRequestWrapper requestWrapper = getMatchingRequest(containerWrapper);
            LOG.info("Found matching request: " + requestWrapper);
            requestWrapper.setAllocatedContainer(containerWrapper);
            if (!requestWrapper.isCancelled()) {
                List<String> startupCommand = requestWrapper.getStartupCommand(installPath, homeDir, deployStreams);
                LOG.info("Command: " + startupCommand);
                containerWrapper.start(startupCommand);
            } else {
                LOG.info("Request has been cancelled. Releasing container: " + containerWrapper);
                releaseContainer(containerWrapper);
            }

        } catch (Exception e) {
            LOG.error("Could not find start container:" + containerWrapper, e);
        }
    }

    private synchronized boolean runAllocate() {
        try {
            NotifyAllocated allocatedNotif = new NotifyAllocated(this);
            NotifyRevoked revokedNotif = new NotifyRevoked(this);

            if (newRequests.size() > 0) {
                LOG.info("Adding new requests, count: " + newRequests.size());
                for (ContainerRequestWrapper crw : newRequests) {
                    LOG.info("Adding new request: " + crw);
                    requestResource(crw);
                    pendingRequests.add(crw);
                }
                newRequests.clear();
            }

            //this call contacts the RM server
            AllocateResponse resp = streamsAMRM.allocate((float)1.0);

            //find out all the new containers that were allocated
            LOG.debug("Response: " + resp.toString());
            LOG.info("Checking for allocated containers: " + resp.getAllocatedContainers().size());
            for (Container container : resp.getAllocatedContainers()) {
                LOG.info("Allocated Container Status: " + container);
                ContainerWrapper containerWrapper = new ContainerWrapper(container, this);
                assignedContainers.put(containerWrapper.getContainer().getId().toString(), containerWrapper);
                containerAssigned(containerWrapper);
            }
            List<ContainerWrapper> toReleaseContainers = new ArrayList<ContainerWrapper>();
            // get the latest status of all assigned containers
            for (ContainerWrapper containerWrapper : assignedContainers.values()) {
                try {
                    ContainerStatus status = nmClient.getContainerStatus(containerWrapper.getContainer().getId(),
                            containerWrapper.getContainer().getNodeId());
                    boolean changed = containerWrapper.setStatus(status);
                    if (changed) {
                        LOG.info("Container: " + containerWrapper + " , status: " + status);
                        if (status.getState() == ContainerState.RUNNING) {
                            if (containerWrapper.isCancelled()) {
                                containerWrapper.stop();
                                LOG.info("Stopping cancelled Container: " + containerWrapper);
                            } else {
                                ResourceDescriptor rd = containerWrapper.getDescriptor();
                                if (toNotifyList.contains(rd.getNativeResourceId())) {
                                    LOG.info("Notifying state changed: " + containerWrapper);
                                    allocatedNotif.getList().add(rd);
                                }
                            }
                        } else if (status.getState() == ContainerState.COMPLETE) {

                            toReleaseContainers.add(containerWrapper);
                            ResourceDescriptor rd = containerWrapper.getDescriptor();
                            if (!containerWrapper.wasShutdown() && !containerWrapper.isCancelled()
                                    && toNotifyList.contains(rd.getNativeResourceId())) {
                                //if it was not stopped by streams, then notify
                                LOG.info("Notifying state changed: " + containerWrapper);
                                revokedNotif.getList().add(rd);
                            }
                        }
                    } else {
                        LOG.debug("Container: " + containerWrapper + " , status: " + status);
                    }
                } catch (YarnException ye) {
                    //happens if the container hasn't been launched yet..print and move on
                    LOG.warn(ye.getMessage());
                }
            }

            NotifyRevokeImminent imminentNotif = new NotifyRevokeImminent(this);
            PreemptionMessage preM = resp.getPreemptionMessage();
            if (preM != null) {
                LOG.info("Preemption Messgae: " + preM);
                if (preM.getStrictContract() != null && preM.getStrictContract().getContainers() != null) {
                    for (PreemptionContainer pc : preM.getStrictContract().getContainers()) {
                        LOG.info("Revoked container: " + pc);
                        ContainerWrapper cw = assignedContainers.get(pc.getId().toString());
                        if (cw != null && toNotifyList.contains(cw.getDescriptor().getNativeResourceId()))
                            imminentNotif.getList().add(cw.getDescriptor());
                    }
                }
                if (preM.getContract() != null && preM.getContract().getContainers() != null) {
                    for (PreemptionContainer pc : preM.getContract().getContainers()) {
                        LOG.info("Revoked container: " + pc);
                        ContainerWrapper cw = assignedContainers.get(pc.getId().toString());
                        if (cw != null && toNotifyList.contains(cw.getDescriptor().getNativeResourceId()))
                            imminentNotif.getList().add(cw.getDescriptor());
                    }
                }
            }

            //release any stopped containers
            if (toReleaseContainers.size() > 0)
                LOG.info("Releasing containers: " + toReleaseContainers.size());
            for (ContainerWrapper c : toReleaseContainers) {
                LOG.debug("Releasing container: " + c);
                releaseContainer(c);
            }

            //add to list of notifications
            if (allocatedNotif.getList().size() > 0) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Adding to allocated notifications: " + allocatedNotif);
                notificationQueue.add(allocatedNotif);
            }
            if (revokedNotif.getList().size() > 0) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Adding to revoked notifications: " + revokedNotif);
                notificationQueue.add(revokedNotif);
            }
            if (imminentNotif.getList().size() > 0) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Adding to immiment revoke notifications: " + imminentNotif);
                notificationQueue.add(imminentNotif);
            }
            //summarize the run
            LOG.info("Containers Summary: " + "Assigned [" + assignedContainers.size() + "], Newly Allocated ["
                    + allocatedNotif.getList().size() + "], Pending [" + pendingRequests.size() + "], Released ["
                    + toReleaseContainers.size() + "], Revoked [" + imminentNotif.getList().size() + "]");

            //return true if we want a smaller wait to quickly ask for pending containers
            return pendingRequests.size() > 0;

        } catch (Exception e) {
            LOG.error(e.toString());
            close();
            System.exit(1);
        }
        return false;
    }

    synchronized void notifyAllocatedAndRunning(List<ResourceDescriptor> allocatedList) {
        if (getResourceNotificationManager() != null && allocatedList.size() > 0) {
            LOG.info("Notifying allocated: " + allocatedList);
            getResourceNotificationManager().resourcesAllocated(clientInfo.getClientId(), allocatedList);
        }
    }

    synchronized void notifyRevoked(List<ResourceDescriptor> revokedList) {
        if (getResourceNotificationManager() != null && revokedList.size() > 0) {
            LOG.info("Notifying revoked: " + revokedList);
            getResourceNotificationManager().resourcesRevoked(clientInfo.getClientId(), revokedList);
        }
    }

    synchronized void notifyRevokeImminent(List<ResourceDescriptor> revokedList) {
        if (getResourceNotificationManager() != null && revokedList.size() > 0) {
            LOG.info("Notifying revoke imminent: " + revokedList);
            getResourceNotificationManager().resourcesRevokeImminent(clientInfo.getClientId(), revokedList);
        }
    }

    @Override
    public synchronized String customCommand(ClientInfo clientInfo, String commandData, Locale locale)
            throws ResourceManagerException {
        this.clientInfo = clientInfo;

        // TODO Auto-generated method stub
        return null;
    }

    static ResourceDescriptor getDescriptor(String id, String host) {
        return new ResourceDescriptor(StreamsYarnConstants.RESOURCE_TYPE, ResourceKind.CONTAINER, id,//native resource name 
                StreamsYarnConstants.RESOURCE_TYPE + "_" + id,//display name
                host);

    }

    static ResourceDescriptorState getDescriptorState(boolean isRunning, ResourceDescriptor rd) {
        AllocateState s = isRunning ? AllocateState.ALLOCATED : AllocateState.PENDING;
        return new ResourceDescriptorState(s, rd);
    }

    private void convertTags(ResourceTags tags, ContainerRequestWrapper crw) throws ResourceTagException,
            ResourceManagerException {
        int cores = -1, memory = -1;

        for (String tag : tags.getNames()) {
            try {
                TagDefinitionType definitionType = tags.getTagDefinitionType(tag);

                switch (definitionType) {
                    case NONE:
                        // use default definition
                        break;

                    case PROPERTIES:
                        Properties propsDef = tags.getDefinitionAsProperties(tag);
                        LOG.info("Tag=" + tag + " props=" + propsDef.toString());
                        if (propsDef.containsKey(StreamsYarnConstants.MEMORY_TAG)) {
                            memory = Math.max(memory, Utils.getIntProperty(propsDef, StreamsYarnConstants.MEMORY_TAG));
                            LOG.info("Tag=" + tag + " memory=" + memory);
                        }

                        if (propsDef.containsKey(StreamsYarnConstants.CORES_TAG)) {
                            cores = Math.max(cores, Utils.getIntProperty(propsDef, StreamsYarnConstants.CORES_TAG));
                            LOG.info("Tag=" + tag + " cores=" + cores);
                        }
                        break;

                    default:
                        throw new ResourceTagException("Tag=" + tag + " has unsupported tag definition type="
                                + definitionType);
                }
            } catch (ResourceException re) {
                throw new ResourceManagerException(re);
            }
        }
        if (memory == -1 && Utils.hasProperty(props, StreamsYarnConstants.PROPS_DC_MEMORY))
            memory = Utils.getIntProperty(props, StreamsYarnConstants.PROPS_DC_MEMORY);

        if (cores == -1 && Utils.hasProperty(props, StreamsYarnConstants.PROPS_DC_CORES))
            cores = Utils.getIntProperty(props, StreamsYarnConstants.PROPS_DC_CORES);

        if (memory != -1) {
            crw.setMemory(memory);
        }
        if (cores != -1) {
            crw.setCpu(cores);
        }
    }

    @Override
    public void validateTags(ClientInfo client, ResourceTags tags, Locale locale) throws ResourceTagException,
            ResourceManagerException {
        for (String tag : tags.getNames()) {
            TagDefinitionType definitionType = tags.getTagDefinitionType(tag);
            switch (definitionType) {
                case NONE:
                    //no definition means use defaults
                    break;
                case PROPERTIES:
                    Properties propsDef = tags.getDefinitionAsProperties(tag);
                    for (Object key : propsDef.keySet()) {
                        validateTagAttribute(tag, (String)key, propsDef.get(key));
                    }
                    break;
                default:
                    Message msg = new Message(Key.ERR_RM_UNSUPPORTED_TAG_DEF_TYPE, StreamsYarnConstants.RESOURCE_TYPE,
                            tag, definitionType);
                    throw new ResourceTagMessageException(msg);
            }
        }
    }

    private void validateTagAttribute(String tag, String key, Object valueObj) throws ResourceTagException {
        //memory, cores
        if (key.equals(StreamsYarnConstants.MEMORY_TAG) || key.equals(StreamsYarnConstants.CORES_TAG)) {
            if (valueObj == null) {
                Message msg = new Message(Key.ERR_RM_REQUIRES_NUMERIC_ATTRIBUTE, StreamsYarnConstants.RESOURCE_TYPE,
                        tag, key, valueObj);
                throw new ResourceTagMessageException(msg);
            } else if (valueObj instanceof String) {
                try {
                    Integer.parseInt(valueObj.toString().trim());
                } catch (NumberFormatException nfe) {
                    Message msg = new Message(Key.ERR_RM_REQUIRES_NUMERIC_ATTRIBUTE,
                            StreamsYarnConstants.RESOURCE_TYPE, tag, key, valueObj);
                    throw new ResourceTagMessageException(msg);
                }
            } else if (!(valueObj instanceof Long) && !(valueObj instanceof Integer)) {
                Message msg = new Message(Key.ERR_RM_REQUIRES_NUMERIC_ATTRIBUTE, StreamsYarnConstants.RESOURCE_TYPE,
                        tag, key, valueObj);
                throw new ResourceTagMessageException(msg);
            }
        } else {
            Message msg = new Message(Key.ERR_RM_UNSUPPORTED_TAG_ATTRIBUTE, StreamsYarnConstants.RESOURCE_TYPE, tag,
                    key);
            throw new ResourceTagMessageException(msg);
        }
    }
}
