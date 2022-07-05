/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.core.ids;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import java.nio.charset.StandardCharsets;

/** Registration ID of remote shuffle components registration like shuffle worker registration. */
public class RegistrationID extends BaseID {

    private static final long serialVersionUID = 390970375272146036L;

    public RegistrationID(byte[] id) {
        super(id);
    }

    public RegistrationID(String id) {
        super(id.getBytes(StandardCharsets.UTF_8));
    }

    public RegistrationID() {
        super(16);
    }

    @Override
    public String toString() {
        return "RegistrationID{" + "ID=" + CommonUtils.bytesToHexString(id) + '}';
    }
}
