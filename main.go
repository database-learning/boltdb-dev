package main

import (
	"bytes"
	"log"

	"github.com/boltdb/bolt"
)

// demo1
func bucketOps() error {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return err
		}
		err = b.Put([]byte("answer"), []byte("42"))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		v := b.Get([]byte("answer"))
		log.Println(string(v))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
	return nil
}

// demo2
func txOps() error {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
	if err != nil {
		log.Fatal(err)
	}
	err = b.Put([]byte("number1"), []byte("43"))
	if err != nil {
		log.Fatal(err)
	}
	err = b.Put([]byte("number2"), []byte("44"))
	if err != nil {
		log.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		v := b.Get([]byte("number1"))
		log.Println(string(v))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

// demo3
func searchOps() error {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		c := b.Cursor()
		prefix := []byte("num")
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			log.Println(string(k), string(v))
		}
		return nil
	})
	return nil
}

func main() {
	bucketOps()
	txOps()
	searchOps()
}
