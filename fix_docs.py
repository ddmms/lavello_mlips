with open("/home/drFaustroll/.gemini/antigravity/brain/4a35b54b-8e6e-4aa8-9d1a-549cfde37aa5/walkthrough.md", "r") as f:
    text = f.read()

# The last addition was the End-of-Run Retry Sweep
text = text.split("**End-of-Run Retry Sweep**")[0].strip()

with open("/home/drFaustroll/.gemini/antigravity/brain/4a35b54b-8e6e-4aa8-9d1a-549cfde37aa5/walkthrough.md", "w") as f:
    f.write(text + "\n")

with open("/home/drFaustroll/.gemini/antigravity/brain/4a35b54b-8e6e-4aa8-9d1a-549cfde37aa5/task.md", "r") as f:
    tlines = f.readlines()

with open("/home/drFaustroll/.gemini/antigravity/brain/4a35b54b-8e6e-4aa8-9d1a-549cfde37aa5/task.md", "w") as f:
    for line in tlines:
        if "Integrate late-stage retry sweep for dropped S3 connections" not in line:
            f.write(line)
