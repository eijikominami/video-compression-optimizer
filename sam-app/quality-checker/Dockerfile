# Quality Checker Lambda with FFmpeg
# Uses AWS Lambda Python 3.11 base image with FFmpeg installed

FROM public.ecr.aws/lambda/python:3.11

# Install FFmpeg from Amazon Linux extras
RUN dnf install -y \
    tar \
    xz \
    && dnf clean all

# Download and install static FFmpeg build
# Using johnvansickle's static builds which are widely used and trusted
RUN curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -o /tmp/ffmpeg.tar.xz \
    && tar -xf /tmp/ffmpeg.tar.xz -C /tmp \
    && mv /tmp/ffmpeg-*-amd64-static/ffmpeg /usr/local/bin/ \
    && mv /tmp/ffmpeg-*-amd64-static/ffprobe /usr/local/bin/ \
    && chmod +x /usr/local/bin/ffmpeg /usr/local/bin/ffprobe \
    && rm -rf /tmp/ffmpeg* \
    && ffmpeg -version

# Copy requirements and install Python dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to the handler
CMD [ "app.lambda_handler" ]
