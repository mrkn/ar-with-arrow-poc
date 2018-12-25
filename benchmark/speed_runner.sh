for i in 1000 2000 3000 5000 10000 20000 30000 50000; do
  echo ===== LIMIT=$i =====
  LIMIT=$i bundle exec benchmark-driver --rbenv '2.5.3' --bundler -r time driver.yml
done
