import cv2

url = "tcp://172.19.160.1:12345"
cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)

while True:
    ret, frame = cap.read()
    if not ret:
        break

    cv2.imshow("stream", frame)

    # ESCキーまたは'q'キーで終了
    key = cv2.waitKey(1) & 0xFF
    if key in (27, ord('q')):   
        break

cap.release()
cv2.destroyAllWindows()