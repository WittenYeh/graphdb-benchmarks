# Copyright 2026 Weitang Ye
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import docker


def build_image(client, dockerfile_path, tag_name, pull=True):
    """
    Builds a docker image from a specific Dockerfile.
    The image is always built and tagged with the specified name (overwriting if exists).

    Args:
        pull: If True, pulls the latest base image from registry before building.
              If False, uses cached base image if available.
    """
    if not os.path.exists(dockerfile_path):
        print(f"Error: Dockerfile not found at {dockerfile_path}")
        sys.exit(1)

    pull_msg = " (pulling latest base image)" if pull else " (using cached base image)"
    print(f"--- Building Image: {tag_name} from {dockerfile_path}{pull_msg} ---")
    try:
        img, logs = client.images.build(
            path=".",
            dockerfile=dockerfile_path,
            tag=tag_name,
            rm=True,
            pull=pull
        )
        return img
    except docker.errors.BuildError as e:
        print(f"Build Failed for {tag_name}:")
        for line in e.build_log:
            if 'stream' in line:
                print(line['stream'].strip())
        sys.exit(1)

def force_remove_container(client, container_name):
    """
    Forcefully removes a container by name if it exists.
    Handles 'removal already in progress' race conditions.
    """
    try:
        container = client.containers.get(container_name)
        print(f"--- Found leftover container '{container_name}', removing... ---")
        try:
            container.stop(timeout=1)
        except: pass

        container.remove(force=True)

    except docker.errors.NotFound:
        pass  # Container does not exist, nothing to do
    except docker.errors.APIError as e:
        # Ignore "removal already in progress" errors (HTTP 409)
        if e.response.status_code == 409 or "already in progress" in str(e):
            print(f"--- Container '{container_name}' is already being removed. Skipping. ---")
        else:
            print(f"Warning: Failed to cleanup {container_name}: {e}")
    except Exception as e:
        print(f"Warning: General error cleaning {container_name}: {e}")

def cleanup_container(container, name):
    """Safe cleanup helper for container objects"""
    if container:
        print(f"--- Stopping container {name}... ---")
        try:
            container.stop()
        except docker.errors.NotFound:
            print(f"Container {name} already removed.")
        except Exception as e:
            print(f"Error stopping {name}: {e}")