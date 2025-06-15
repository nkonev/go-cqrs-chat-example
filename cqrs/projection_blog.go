package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-cqrs-chat-example/db"
	"time"
)

func (m *CommonProjection) refreshBlog(ctx context.Context, tx *db.Tx, chatId int64, createdTime time.Time) error {
	_, errInner := tx.ExecContext(ctx, `
				with blog_message as (
					select m.* from message m where m.chat_id = $1 and m.blog_post = true
				)	
				insert into blog(id, owner_id, title, post, preview, create_date_time)
				select 
				    cast ($1 as bigint), 
				    (select m.owner_id from blog_message m),
				    (select c.title from chat_common c where c.id = $1),
				    (select m.content from blog_message m),
				    (select left(strip_tags(m.content), $2) from blog_message m),
					$3
				on conflict(id) do update set owner_id = excluded.owner_id, title = excluded.title, post = excluded.post, preview = excluded.preview, create_date_time = excluded.create_date_time
			`, chatId, 512, createdTime)
	if errInner != nil {
		return errInner
	}
	return nil
}

func (m *CommonProjection) OnMessageBlogPostMade(ctx context.Context, event *MessageBlogPostMade) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		chatExists, errInner := m.checkChatExists(ctx, tx, event.ChatId)
		if errInner != nil {
			return errInner
		}
		if !chatExists {
			m.lgr.WithTrace(ctx).Info("Skipping MessageBlogPostMade because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		messageExists, errInner := m.checkMessageExists(ctx, tx, event.ChatId, event.MessageId)
		if errInner != nil {
			return errInner
		}
		if !messageExists {
			m.lgr.WithTrace(ctx).Info("Skipping MessageBlogPostMade because there is no message", "chat_id", event.ChatId, "message_id", event.MessageId)
			return nil
		}

		// unset previous
		_, errInner = tx.ExecContext(ctx, "update message set blog_post = false where chat_id = $1 and id = (select id from message where chat_id = $1 and blog_post = true)", event.ChatId)
		if errInner != nil {
			return errInner
		}

		_, errInner = tx.ExecContext(ctx, "update message set blog_post = $3 where chat_id = $1 and id = $2", event.ChatId, event.MessageId, event.BlogPost)
		if errInner != nil {
			return errInner
		}

		errInner = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt)
		if errInner != nil {
			return errInner
		}
		return nil
	})

	return errOuter
}

func (m *CommonProjection) isChatBlog(ctx context.Context, co db.CommonOperations, chatId int64) (bool, error) {
	r := co.QueryRowContext(ctx, "select exists(select * from chat_common where id = $1 and blog = true)", chatId)
	if r.Err() != nil {
		return false, r.Err()
	}
	var blog bool
	err := r.Scan(&blog)
	if err != nil {
		return false, err
	}
	return blog, nil
}

func (m *CommonProjection) isMessageBlogPost(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (bool, error) {
	r := co.QueryRowContext(ctx, "select exists(select * from message where chat_id = $1 and id = $2 and blog_post = true order by id desc limit 1)", chatId, messageId)
	var blog bool
	err := r.Scan(&blog)
	if err != nil {
		return false, err
	}
	return blog, nil
}

// list view
type BlogViewDto struct {
	Id             int64     `json:"id"`
	OwnerId        *int64    `json:"ownerId"`
	Title          string    `json:"title"`
	Preview        *string   `json:"preview"`
	CreateDateTime time.Time `json:"createDateTime"`
}

func (m *CommonProjection) GetBlogs(ctx context.Context, size int32, offset int64, reverseOrder bool) ([]BlogViewDto, error) {
	ma := []BlogViewDto{}

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	rows, err := m.db.QueryContext(ctx, fmt.Sprintf(`
		select 
		    b.id,
			b.owner_id,
		    b.title,
		    b.preview,
		    b.create_date_time
		from blog b
		order by b.create_date_time %s
		limit $1 offset $2
	`, order), size, offset)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd BlogViewDto
		err = rows.Scan(&cd.Id, &cd.OwnerId, &cd.Title, &cd.Preview, &cd.CreateDateTime)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}

type BlogDto struct {
	Id             int64     `json:"id"`
	OwnerId        *int64    `json:"ownerId"`
	Title          string    `json:"title"`
	Post           *string   `json:"post"`
	CreateDateTime time.Time `json:"createDateTime"`
}

func (m *CommonProjection) GetBlog(ctx context.Context, blogId int64) (*BlogDto, error) {
	row := m.db.QueryRowContext(ctx, `
		select 
		    b.id,
			b.owner_id,
		    b.title,
		    b.post,
		    b.create_date_time
		from blog b
		where b.id = $1
		order by b.create_date_time desc 
	`, blogId)
	if row.Err() != nil {
		if errors.Is(row.Err(), sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return nil, nil
		}
		return nil, row.Err()
	}

	var cd BlogDto
	err := row.Scan(&cd.Id, &cd.OwnerId, &cd.Title, &cd.Post, &cd.CreateDateTime)
	if err != nil {
		return nil, err
	}

	return &cd, nil
}

func (m *CommonProjection) getBlogPostMessageId(ctx context.Context, co db.CommonOperations, blogId int64) (int64, error) {
	res := co.QueryRowContext(ctx, "select id from message where chat_id = $1 and blog_post = true order by id desc limit 1", blogId)
	var messageId int64
	if err := res.Scan(&messageId); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return 0, nil
		}
		return 0, err
	}
	return messageId, nil
}

type CommentViewDto struct {
	Id             int64      `json:"id"`
	OwnerId        int64      `json:"ownerId"`
	Content        string     `json:"content"`
	CreateDateTime time.Time  `json:"createDateTime"`
	UpdateDateTime *time.Time `json:"editDateTime"` // for sake compatibility
}

func (m *CommonProjection) getComments(ctx context.Context, co db.CommonOperations, blogId, postMessageId int64, size int32, offset int64, reverseOrder bool) ([]CommentViewDto, error) {
	ma := []CommentViewDto{}

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	rows, err := co.QueryContext(ctx, fmt.Sprintf(`
		select id, owner_id, content, create_date_time, update_date_time
		from message 
		where chat_id = $1 and id > $2
		order by id %s
		limit $3 offset $4
	`, order), blogId, postMessageId, size, offset)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd CommentViewDto
		err = rows.Scan(&cd.Id, &cd.OwnerId, &cd.Content, &cd.CreateDateTime, &cd.UpdateDateTime)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}

func (m *CommonProjection) GetComments(ctx context.Context, blogId int64, size int32, offset int64, reverseOrder bool) ([]CommentViewDto, error) {
	res, errOuter := db.TransactWithResult(ctx, m.db, func(tx *db.Tx) ([]CommentViewDto, error) {
		postMessageId, err := m.getBlogPostMessageId(ctx, tx, blogId)
		if err != nil {
			return []CommentViewDto{}, err
		}
		comments, err := m.getComments(ctx, tx, blogId, postMessageId, size, offset, reverseOrder)
		if err != nil {
			return []CommentViewDto{}, err
		}
		return comments, nil
	})
	if errOuter != nil {
		return []CommentViewDto{}, errOuter
	}
	return res, nil
}
