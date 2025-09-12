# Dockerfile.base
FROM alpine:3.19

# Install only essential packages (~10MB total)
RUN apk add --no-cache \
    openssh-client \
    openssh-server \
    sudo \
    bash \
    && rm -rf /var/cache/apk/*

# Setup SSH for passwordless access
RUN ssh-keygen -A && \
    sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    echo "root:root" | chpasswd

# Create workspace directory
RUN mkdir -p /opt/mapreduce /root/.ssh

# Keep container running and start SSH
CMD ["/usr/sbin/sshd", "-D", "-e"]

EXPOSE 22
