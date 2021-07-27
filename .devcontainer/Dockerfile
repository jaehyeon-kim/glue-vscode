FROM amazon/aws-glue-libs:glue_libs_1.0.0_image_01

ARG USERNAME
ARG USER_UID
ARG USER_GID

## Create non-root user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

## Add sudo support in case we need to install software after connecting
## Jessie is not the latest stable Debian release - jessie-backports is not available
RUN rm -rf /etc/apt/sources.list.d/jessie-backports.list

RUN apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
