/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import org.hillview.*;
import org.hillview.targets.HillViewHostDisplay;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.management.*;
import org.hillview.maps.FindFilesMapper;
import org.hillview.maps.LoadDatabaseTableMapper;
import org.hillview.storage.*;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class InitialObjectTarget extends RpcTarget {
    private static final String LOCALHOST = "127.0.0.1";
    private static final String ENV_VARIABLE = "WEB_CLUSTER_DESCRIPTOR";

    @Nullable
    private IDataSet<Empty> emptyDataset = null;
    private IDataSet<Empty> emptyDataset1 = null;

    public HillViewHostDisplay hosts = new HillViewHostDisplay();
    
    ArrayList<IDataSet<Empty>> newHosts=new ArrayList<IDataSet<Empty>>();
    
    HostList desc1;

    public InitialObjectTarget() {
        // Get the base naming context
        final String clusterFile = System.getenv(ENV_VARIABLE);
        final HostList desc;
	try{
	hosts.getTagsHosts("dev");
	}
	catch(Exception e){
	}
        if (clusterFile == null) {
            HillviewLogger.instance.info(
                    "No cluster description file specified; creating singleton");
            desc = new HostList(Collections.singletonList(HostAndPort.fromParts(LOCALHOST,
                                                                    HillviewServer.DEFAULT_PORT)));
            this.initialize(desc);
        } else {
            try {
                HillviewLogger.instance.info(
                        "Initializing cluster descriptor from file", "{0}", clusterFile);
                desc = HostList.fromFile(clusterFile);
                HillviewLogger.instance.info("Backend servers", "{0}", desc.getServerList().size());
                this.initialize(desc);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        RpcObjectManager.instance.addObject(this);
    }

    private void initialize(final HostList description) {
        this.emptyDataset = RemoteDataSet.createCluster(description, RemoteDataSet.defaultDatasetIndex);
    }

    @HillviewRpc
    public void loadDBTable(RpcRequest request, RpcRequestContext context) {
        JdbcConnectionInformation conn = request.parseArgs(JdbcConnectionInformation.class);
        LoadDatabaseTableMapper mapper = new LoadDatabaseTableMapper(conn);
        assert this.emptyDataset != null;
        this.runMap(this.emptyDataset, mapper, TableTarget::new, request, context);
    }

    @HillviewRpc
    public void findFiles(RpcRequest request, RpcRequestContext context) {
        Utilities u = new Utilities();
        FileSetDescription desc = request.parseArgs(FileSetDescription.class);
        HillviewLogger.instance.info("Finding files", "{0}", desc.getTags());
	
	try{
        	hosts.getTagsHosts(desc.getTags());
		Map m=hosts.getTagsValues();
		HillviewLogger.instance.info("Hosts----","{0}",m);
		u.setTagsForHost(m);
		HillviewLogger.instance.info("Hosts----","{0}",u.getTagsForHost());
		
		desc1 = HostList.fromFile("newHosts");
	}
	catch(Exception e){
		HillviewLogger.instance.info("Error in getting hosts", "{0}", e.getLocalizedMessage());
	}
	this.emptyDataset1 = RemoteDataSet.createCluster(desc1, RemoteDataSet.defaultDatasetIndex);
        IMap<Empty, List<IFileReference>> finder = new FindFilesMapper(desc);
        assert this.emptyDataset1 != null;
        this.runFlatMap(this.emptyDataset1, finder, FileDescriptionTarget::new, request, context);
    }

    @HillviewRpc
    public void findLogs(RpcRequest request, RpcRequestContext context) {
	FileSetDescription desc = new FileSetDescription();
	 Utilities u=new Utilities();
	try{
        	
		Map m=hosts.getTagsValues();
		HillviewLogger.instance.info("Hosts----","{0}",m);
		u.setTagsForHost(m);
		HillviewLogger.instance.info("Hosts----","{0}",u.getTagsForHost());
		
		desc1 = HostList.fromFile("newHosts");
	}
	catch(Exception e){
		HillviewLogger.instance.info("Error in getting hosts", "{0}", e.getLocalizedMessage());
	}
	this.emptyDataset1 = RemoteDataSet.createCluster(desc1, RemoteDataSet.defaultDatasetIndex);
        @Nullable String cookie = request.parseArgs(String.class);
        desc.cookie = cookie;
        desc.fileKind = "hillviewlog";
        desc.fileNamePattern = "hillview*.log";
        desc.repeat = 1;
        desc.folder = ".";  // relative to the work directory of the worker process
        IMap<Empty, List<IFileReference>> finder = new FindFilesMapper(desc);
        HillviewLogger.instance.info("Finding log files");
        assert this.emptyDataset1 != null;
        this.runFlatMap(this.emptyDataset1, finder, FileDescriptionTarget::new, request, context);
    }

    @Override
    public String toString() {
        return "Initial object=" + super.toString();
    }

    //--------------------------------------------
    // Management messages

    @HillviewRpc
    public void ping(RpcRequest request, RpcRequestContext context) {
        PingSketch<Empty> ping = new PingSketch<Empty>();
        this.runSketch(Converters.checkNull(this.emptyDataset), ping, request, context);
    }

    @HillviewRpc
    public void setMemoization(RpcRequest request, RpcRequestContext context) {
        SetMemoization tm = new SetMemoization(true);
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void unsetMemoization(RpcRequest request, RpcRequestContext context) {
        SetMemoization tm = new SetMemoization(false);
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void purgeMemoization(RpcRequest request, RpcRequestContext context) {
        PurgeMemoization tm = new PurgeMemoization();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void purgeLeafDatasets(RpcRequest request, RpcRequestContext context) {
        PurgeLeafDatasets tm = new PurgeLeafDatasets();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void memoryUse(RpcRequest request, RpcRequestContext context) {
        MemoryUse tm = new MemoryUse();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void purgeDatasets(RpcRequest request, RpcRequestContext context) {
        int deleted = RpcObjectManager.instance.removeAllObjects();
        ControlMessage.Status status = new ControlMessage.Status("Deleted " + deleted + " objects");
        JsonList<ControlMessage.Status> statusList = new JsonList<ControlMessage.Status>();
        statusList.add(status);
        PartialResult<JsonList<ControlMessage.Status>> pr = new PartialResult<JsonList<ControlMessage.Status>>(statusList);
        RpcReply reply = request.createReply(Utilities.toJsonTree(pr));
        Session session = context.getSessionIfOpen();
        if (session == null)
            return;
        RpcServer.sendReply(reply, session);
        RpcServer.requestCompleted(request, session);
        request.syncCloseSession(session);
    }
}
