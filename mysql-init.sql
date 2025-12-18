CREATE DATABASE IF NOT EXISTS db_makan;
USE db_makan;

CREATE TABLE riwayat_makan (
  id_makan INT NOT NULL AUTO_INCREMENT,
  tanggal DATE NOT NULL,
  waktu TIME NOT NULL,
  id_warung INT DEFAULT NULL,
  nama_warung VARCHAR(100) DEFAULT NULL,
  menu VARCHAR(100) DEFAULT NULL,
  kategori VARCHAR(50) DEFAULT NULL,
  harga INT DEFAULT NULL,
  metode VARCHAR(20) DEFAULT NULL,
  kepuasan INT DEFAULT NULL,
  PRIMARY KEY (id_makan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO riwayat_makan (id_makan, tanggal, waktu, id_warung, nama_warung, menu, kategori, harga, metode, kepuasan) VALUES
(1, '2025-12-15', '12:05:00', 16, 'Kantin Teknik', 'Gado-gado', 'Gorengan', 18000, 'dine-in', 4),
(2, '2025-12-14', '14:12:00', 16, 'Kantin Teknik', 'Nasi Soup', 'Nasi', 15000, 'dine-in', 4),
(3, '2025-12-13', '15:00:00', 0, 'Richesee Kayu tangi', 'Nasi ayam', 'Nasi', 45000, 'dine-in', 5),
(4, '2025-12-12', '11:45:00', 16, 'Kantin Teknik', 'Nasi ayam geprek tanpa sambal dan mie', 'Nasi', 15000, 'dine-in', 5),
(5, '2025-12-11', '17:00:00', 0, 'd''BestO', 'Nasi Box Ayam Geprek ', 'Nasi', 17500, 'takeaway', 4),
(6, '2025-12-10', '11:00:00', 0, 'Warung Nasi Goreng Kaisar', 'Ayam asam manis dan tahu dan sayur kacang', 'Nasi', 12000, 'takeaway', 5),
(7, '2025-12-09', '13:15:00', 0, 'Ayam Gemprek Meratus Mulawarman', 'Ayam Gemprek Mozzarella cabe 1', 'Nasi', 24000, 'takeaway', 4),
(8, '2025-12-08', '12:44:00', 0, 'RM Uda Sayang', 'Nasi Ayam Gulai', 'Nasi', 20000, 'takeaway', 5),
(9, '2025-12-07', '17:55:00', 0, 'Warung Makan Ayam Geprek Agus', 'ayam geprek', 'Nasi', 19000, 'dine-in', 5),
(10, '2025-12-06', '13:45:00', 16, 'Kantin Teknik', 'mie goreng aceh 2 bungkus + telur dadar', 'Mie', 16000, 'dine-in', 5),
(11, '2025-12-05', '11:00:00', 0, 'Bitten Coffee izakaya', 'Ricebowl ayam asam manis 2', 'Nasi', 25000, 'dine-in', 5),
(12, '2025-12-04', '15:00:00', 8, 'Warung Yuliriska', 'Ayam Cabe Ijo', 'Nasi', 15000, 'dine-in', 5),
(13, '2025-12-03', '11:20:00', 8, 'Warung Yuliriska', 'Ayam Goreng Sambal Ijo', 'Nasi', 13000, 'takeaway', 5),
(14, '2025-12-02', '12:00:00', 16, 'Kantin Teknik ', 'Ayam bistik dada pakai nasi sayur tempe air hangat', 'Nasi', 13000, 'dine-in', 4),
(15, '2025-12-01', '01:15:00', 0, 'Nasi Padang cendana', 'Nasi Padang lauk perkedel', 'Nasi', 13000, 'takeaway', 5),
(16, '2025-11-30', '13:20:00', 3, 'Warung Makan Bu Darmi ', 'Ayam Goreng', 'Nasi', 13000, 'dine-in', 4),
(17, '2025-11-29', '12:34:00', 0, 'Mie Ayam & Bakso Subur Rejeki', 'mie ayam', 'Mie', 15000, 'dine-in', 5),
(18, '2025-11-28', '14:00:00', 16, 'Kantin Teknik', 'Nasi dengan lauk ayam bistik', 'Nasi', 13000, 'dine-in', 4),
(19, '2025-11-27', '14:30:00', 0, 'Warung Nasi Goreng Kaisar', 'Ayam Panggang', 'Nasi', 20000, 'takeaway', 4),
(20, '2025-11-26', '14:00:00', 4, 'Warung Umar bin Khattab', 'Kari Ayam', 'Nasi', 13000, 'takeaway', 4),
(21, '2025-12-15', '13:00:00', 3, 'Warung Makan Bu Darmi ', 'Katsu', 'Nasi', 13000, 'takeaway', 4),
(22, '2025-12-16', '22:54:30', 0, 'Warung Nasi Goreng Kaisar', 'Nasi Campur Amay', 'Nasi', 15000, 'Dine-in', 4),
(23, '2025-12-16', '23:17:04', NULL, 'Wizzmie Banjarmasin', 'Mie Hompimpa', 'Mie', 15000, 'Dine-in', 5);

