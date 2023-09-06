Ini adalah final code setelah mengikuti materi DE-Zoomcamp Week 1 dari DataTalksClub. Pada folder ini terdapat 2 bagian yaitu:
1. Docker & SQL
2. GCP & Terraform

### 1. Docker & SQL
Materi pada bagian ini bertujuan untuk menjalankan proses ETL sederhana (dengan Python) melalui docker container, data tersebut di load ke PostgreSQL dan kemudian dapat dilihat melalui pgAdmin 4. Container PostgreSQL dan pgAdmin dijalankan bersamaan dengan docker-compose. 
1. Pastikan Docker telah terisntall
2. Buka terminal (saya akan menggunakan gitbash) pada folder directory anda. Bisa dengan command berikut ini:

```
cd 'path'
```

4. Dalam repo ini terdapat file docker-compose.yaml.

![Docker Compose yaml](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/6da58ea7-a6af-4cf7-9535-29ccc561f97f)

Ketika dijalankan, file ini akan menjalankan layanan PostgreSQL dan pgAdmin secara bersamaan dengan konfigurasi yang sudah ditentukan. 
Layanan pgdatabase menggunakan image postgres:13, dengan environment variable POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB are set to root, root, and ny_taxi. Volumes digunakan untuk mendefinisikan dan mengaitkan volume Docker dengan kontainer, bagian-bagiannya dipisahkan dengan tanda titik dua (':'). Bagian pertama  (./ny_taxi_postgres_data) adalah path pada host  yang akan dihubungkan atau dipetakan ke volume Docker. Bagian kedua (/var/lib/postgresql/data) adalah path di dalam kontainer PostgreSQL di mana volume tersebut akan muncul. Bagian ketiga ('rw') memeberi akses pada kontainer berupa read and write pada volume. Ports disini menghubungkan port pada host dengan container, disini saya menggunakan port 5431 (port 5432 sudah dipakai oleh PostgreSQL pada machine saya) pada host untuk terhubung ke port 5432 pada container.

Layanan pgadmin menggunakan image dpage/pgadmin4, dengan environment variable PGADMIN_DEFAULT_EMAIL, PGADMIN_DEFAULT_PASSWORD adalah admin@admin.com dan root. Penjelasan volumes sendiri sama seperti layanan pgdatabase. Port yang digunakan adalah 8080 pada host dan 80 pada container.

Kemudian, bagian volumes di bawah berfungsi untuk menyimpan data di host (driver: local). Hal ini dimaksudkan agar setiap menjalankan container baru tidak perlu untuk membuat dan mengkonfigurasi koneksi pgdatabase server di pgAdmin.

Jalankan command code dibawah ini untuk menjalankan docker-compose:

```
docker-compose up
```
5. Membuat image taxi_ingest dan menjalankan containernya



