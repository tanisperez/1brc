#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Uncomment below to use sdk
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.2-graal 1>&2

# ./mvnw clean verify removes target/ and will re-trigger native image creation.
if [ ! -f target/CalculateAverage_tanisperez_image ]; then
    # Performance tuning flags, optimization level 3, maximum inlining exploration, and compile for the architecture where the native image is generated.
    NATIVE_IMAGE_OPTS="-O3 -H:TuneInlinerExploration=1 -H:+UnlockExperimentalVMOptions -march=native"

    NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS --enable-preview --initialize-at-build-time=dev.morling.onebrc.CalculateAverage_tanisperez"

    # My MacBook is not able to run the challenge without Garbage Collector, so we will use the default Serial GC.
    NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS -H:-GenLoopSafepoints"

    # Uncomment the following line for outputting the compiler graph to the IdealGraphVisualizer
    # NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS -H:MethodFilter=CalculateAverage_tanisperez.* -H:Dump=:2 -H:PrintGraph=Network"

    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_tanisperez_image dev.morling.onebrc.CalculateAverage_tanisperez
fi
