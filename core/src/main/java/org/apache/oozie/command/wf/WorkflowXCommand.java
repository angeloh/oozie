/**
* Copyright (c) 2010 Yahoo! Inc. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License. See accompanying LICENSE file.
*/
package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.jpa.WorkflowActionInsertCommand;
import org.apache.oozie.command.jpa.WorkflowActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowJobInsertCommand;
import org.apache.oozie.command.jpa.WorkflowJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

public abstract class WorkflowXCommand<T> extends XCommand<T> {

    /**
     * @param name command name.
     * @param type command type.
     * @param priority priority of the command, used when queuing for asynchronous execution.
     */
    public WorkflowXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    public void insertJobToDB(WorkflowJobBean wfJob) throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                jpaService.execute(new WorkflowJobInsertCommand(wfJob));
            }
            else {
                getLog().error(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    public void updateJobToDB(WorkflowJobBean wfJob) throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
            }
            else {
                getLog().error(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    public void insertActionToDB(WorkflowActionBean wfAction) throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                jpaService.execute(new WorkflowActionInsertCommand(wfAction));
            }
            else {
                getLog().error(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    public void updateActionToDB(WorkflowActionBean wfAction) throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
            }
            else {
                getLog().error(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

}