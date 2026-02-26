# Stoc_Kis → GitHub 푸시 가이드

## 1. GitHub에 SSH 키 등록 (1회만)

EC2에 SSH 접속 후 아래 공개 키를 GitHub에 등록하세요.

**공개 키 (복사하여 사용):**
```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEXrZERW/M4lWT+HDtMqcGMR2I+VJFp4RybPKDFluU1T sywems@gamil.com
```

**등록 절차:**
1. https://github.com/settings/keys 접속
2. "New SSH key" 클릭
3. Title: `EC2-Stoc_Kis` (또는 원하는 이름)
4. Key: 위 공개 키 전체 붙여넣기
5. Add SSH key

---

## 2. GitHub에 Stoc_Kis 리포지토리 생성 (없는 경우)

1. https://github.com/new 접속
2. Repository name: `Stoc_Kis`
3. Public 선택
4. **"Add a README file" 등 체크하지 말고** Create (빈 저장소로 생성)

---

## 3. EC2에서 푸시

```bash
cd /home/ubuntu/Stoc_Kis
git push -u origin main
```

---

## 현재 상태

- [x] git init 완료
- [x] .gitignore 생성 (data/, config.json, kis_token*.json, venv 등 제외)
- [x] 69개 프로젝트 파일 커밋 완료
- [x] remote origin: git@github.com:song-71/Stoc_Kis.git
- [ ] SSH 키 GitHub 등록 (사용자 작업)
- [ ] Stoc_Kis 리포지토리 생성 (없는 경우)
- [ ] git push

---

## _DevProj에 올리려면

Stoc_Kis를 _DevProj 내부 폴더로 올리려면:

```bash
cd /home/ubuntu
git clone git@github.com:song-71/_DevProj.git
cp -r Stoc_Kis/* _DevProj/Stoc_Kis/   # _DevProj/Stoc_Kis 폴더 생성 후 복사
cd _DevProj
git add Stoc_Kis/
git commit -m "Add Stoc_Kis project"
git push origin main
```
