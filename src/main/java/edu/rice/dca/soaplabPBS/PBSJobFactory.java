package edu.rice.dca.soaplabPBS;
// SowaJobFactory.java
//
// Created: April 2007
//
// Copyright 2007 Martin Senger
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


import org.soaplab.share.SoaplabException;
import org.soaplab.services.Reporter;
import org.soaplab.services.metadata.MetadataAccessor;
import org.soaplab.services.JobFactory;
import org.soaplab.services.Job;

import java.util.Map;

/**
 * A factory for SowaJob instances. <p>
 *
 * @author <A HREF="mailto:martin.senger@gmail.com">Martin Senger</A>
 * @version $Id: SowaJobFactory.java,v 1.2 2007/05/16 13:13:26 marsenger Exp $
 */

public class PBSJobFactory
    implements JobFactory {

    /**************************************************************************
     * The default constructor.
     **************************************************************************/
    public PBSJobFactory() {
    }

    /**************************************************************************
     * FACTORY: Get a new Job. <p>
     *
     **************************************************************************/
    public Job newInstance (String jobId,
			    MetadataAccessor metadataAccessor,
			    Reporter reporter,
			    Map<String,Object> sharedAttributes,
			    boolean jobRecreated)
	throws SoaplabException {
	return new PBSJob (jobId, metadataAccessor, reporter,
			    sharedAttributes, jobRecreated);
    }
}
