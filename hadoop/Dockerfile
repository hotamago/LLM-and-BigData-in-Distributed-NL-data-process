# Use the official Spark image as the base
FROM apache/hadoop:3.4.1

USER root

# Create the Hadoop data directory
RUN mkdir -p /hadoop/data
RUN mkdir -p /hadoop/name

# Add permissions to the Hadoop data directory
RUN chown -R hadoop:hadoop /hadoop/name
RUN chown -R hadoop:hadoop /hadoop/data

# Back to the default user
USER hadoop