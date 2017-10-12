/*
 * Copyright 2015-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const DOCKER_MACHINE = '192.168.99.100';

exports.druidHost = `${DOCKER_MACHINE}:8082`;
exports.druidVersion = '0.11.0-SNAPSHOT';

exports.liveDruidHost = `172.31.${2567/500}:8082`;
exports.liveDruidVersion = '0.10.0-iap1';
