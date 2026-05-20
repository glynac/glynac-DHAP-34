CREATE TABLE IF NOT EXISTS public.chatbot_conversations (
    id        INTEGER NOT NULL,
    question  TEXT    NOT NULL,
    answer    TEXT    NOT NULL,
    PRIMARY KEY (id)
);
