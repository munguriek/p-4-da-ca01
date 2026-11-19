# Alibaba cluster trace v2018: machine_meta and container_meta
Publisher and URL: Alibaba Group, https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/trace_2018.md
Licence or terms: research or study use only, per the publisher's README; never redistributed: the files stay in data/, which Git ignores
How it arrives (stream, poll or file) and how often: file, downloaded once, from the link given after the publisher's short survey
Format and version: headerless CSV inside .tar.gz, with columns as in cluster-trace-v2018/schema.txt; trace v2018, about 4,000 machines over 8 days
Personal data, and how it is handled: none: machines and containers only
Known issues: times are seconds from the start of the trace, not dates; memory and disk are rescaled to 0-100, with -1 or 101 marking invalid values
Checksum and download date (files): machine_meta.tar.gz, SHA-256 b5b1b786b22cd413a3674b8f2ebfb2f02fac991c95df537f363ef2797c8f6d55; container_meta.tar.gz, SHA-256 febd75e693d1f208a8941395e7faa7e466e50d21c256eff12a815b7e2fa2053f; both match the publisher's values; downloaded TODO
Registered on: TODO
