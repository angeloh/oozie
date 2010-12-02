/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command;

import org.apache.oozie.client.Job;
import org.apache.oozie.util.XLog;

public abstract class StartTransitionXCommand extends TransitionXCommand<Void> {

    protected final XLog LOG = XLog.getLog(KillTransitionXCommand.class);

    private Job job;

    private Job.Status childStatus;
    private boolean childPending;

    public abstract void StartChildren() throws CommandException;

    public StartTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    public StartTransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    @Override
    public void transitToNext() {
        job.setStatus(Job.Status.RUNNING);
        job.setPending();
        childStatus = Job.Status.PREP;
        childPending = true;
    }

    @Override
    protected Void execute() throws CommandException {
        loadState();
        job = this.getJob();
        transitToNext();
        StartChildren();
        notifyParent();
        return null;
    }

    /**
     * @return the childStatus
     */
    public Job.Status getChildStatus() {
        return childStatus;
    }

    /**
     * @param childStatus the childStatus to set
     */
    public void setChildStatus(Job.Status childStatus) {
        this.childStatus = childStatus;
    }

    /**
     * @return the childPending
     */
    public boolean isChildPending() {
        return childPending;
    }

    /**
     * @param childPending the childPending to set
     */
    public void setChildPending(boolean childPending) {
        this.childPending = childPending;
    }
}
