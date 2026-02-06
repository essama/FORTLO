import base64

with open("white_back.png", "rb") as img:
    encoded = base64.b64encode(img.read()).decode("utf-8")

def get_logo_encoding():
    return encoded
