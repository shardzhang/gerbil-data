# Data Overview

ml-1m

Anonymized dataset containing 1,000,209 ratings from 6,040 users on approximately 3,900 movies.

## movies.dat

Movie information

`MovieID::Title::Genres`

- MovieID: Movie ID, range 1-3952
- Title: Movie name (with release year)
- `Genres`: Genre(s), separated by `|`. Action, Adventure, Animation, Children's, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western

```bash
~/Py/data/ml-1m  head movies.dat
1::Toy Story (1995)::Animation|Children's|Comedy
2::Jumanji (1995)::Adventure|Children's|Fantasy
3::Grumpier Old Men (1995)::Comedy|Romance
4::Waiting to Exhale (1995)::Comedy|Drama
5::Father of the Bride Part II (1995)::Comedy
6::Heat (1995)::Action|Crime|Thriller
7::Sabrina (1995)::Comedy|Romance
8::Tom and Huck (1995)::Adventure|Children's
9::Sudden Death (1995)::Action
10::GoldenEye (1995)::Action|Adventure|Thriller
```

## users.dat

Format: `UserID::Gender::Age::Occupation::Zip-code`

- User ID, range 1 - 6040

- Gender: M (Male) / F (Female)

- Age: Age range (encoded):

  1 → Under 18

  18 → 18-24

  25 → 25-34

  35 → 35-44

  45 → 45-49

  50 → 50-55

  56 → 56+

- Occupation (encoded):

  0 → Other / Not specified

  1 → Academic / Educator

  2 → Artist

  3 → Clerical / Administrative

  4 → College / Graduate student

  5 → Customer service

  6 → Doctor / Healthcare

  7 → Executive / Managerial

  8 → Farmer

  9 → Homemaker

  10 → K-12 student

  11 → Lawyer

  12 → Programmer

  13 → Retired

  14 → Sales / Marketing

  15 → Scientist

  16 → Self-employed

  17 → Technician / Engineer

  18 → Tradesman / Craftsman

  19 → Unemployed

  20 → Writer

- Zip code (self-reported by users, not validated for accuracy)

```bash
 ~/Py/data/ml-1m  head users.dat
1::F::1::10::48067
2::M::56::16::70072
3::M::25::15::55117
4::M::45::7::02460
5::M::25::20::55455
6::F::50::9::55117
7::M::35::1::06810
8::M::25::12::11413
9::M::25::17::61614
10::F::35::1::95370
```

## ratings.dat

Format: `UserID::MovieID::Rating:Timestamp`

- `UserID`: User ID, range 1 - 6040
- `MovieID`: Movie ID, range 1 - 3952
- `Rating`: Rating on a 5-star scale (integer, 1-5 stars)
- `Timestamp`: Unix timestamp in seconds

Note: Each user has at least 20 rating records.

```bash
~/Py/data/ml-1m  head ratings.dat
1::1193::5::978300760
1::661::3::978302109
1::914::3::978301968
1::3408::4::978300275
1::2355::5::978824291
1::1197::3::978302268
1::1287::5::978302039
1::2804::5::978300719
1::594::4::978302268
1::919::4::978301368
```

Binary classification task: Model user click-through rate as click probability. Ratings >= 3 are treated as positive samples. Ratings < 3 are treated as negative samples.

Data tables:

1. `ml_1m_item_tabel`: Item table
2. `ml_1m_user_tabel`: User table
3. `ml_1m_label_tabel`: Label table
4. `ml_1m_user_behavior_tabel`: User behavior sequence table
5. `ml_1m_train_sample_binary`: Sample table (binary classification)
6. `ml_1m_train_sample_multiclass`: Sample table (multi-class classification)
