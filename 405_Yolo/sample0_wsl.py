import cv2

cap = cv2.VideoCapture(0, cv2.CAP_V4L2)
# cap = cv2.VideoCapture(1, cv2.CAP_V4L2)
cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('M','J','P','G'))
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
cap.set(cv2.CAP_PROP_FPS, 30)

print("isOpened:", cap.isOpened())
ret, frame = cap.read()
print("ret:", ret)

if ret:
    cv2.imwrite("test.jpg", frame)
    print("test.jpg 保存成功！")
else:
    print("フレーム取得失敗")

cap.release()

