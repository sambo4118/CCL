import fs from "node:fs";
import path from 'path';
import { fileURLToPath } from 'node:url';
import 'dotenv/config';
import session from 'express-session';
import Database from 'better-sqlite3';
import betterSqlite3SessionStoreFactory from 'better-sqlite3-session-store';
import passport from 'passport';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';

export function setupSessionStore(app) {

    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const SQLiteStore = betterSqlite3SessionStoreFactory(session);

    const sessionDb = new Database(path.join(__dirname, 'sessions.db'));

    app.use(session({
        store: new SQLiteStore({
            client: sessionDb,
            expired: {
                clear: true,
                intervalMs: 1000 * 60 * 15 // Clear expired sessions every 15 minutes
            }
        }),
        secret: process.env.SESSION_SECRET,
        resave: false,
        saveUninitialized: false,
        cookie: {
            httpOnly: true,
            maxAge: 1000 * 60 * 60 * 24 * 7 * 3 // 3 weeks
        }
    }));

    return { sessionDb, SQLiteStore };
}

export function setupPassport(app) {
    passport.use(new GoogleStrategy({
        clientID: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        callbackURL: '/auth/google/callback',
        userProfileURL: 'https://www.googleapis.com/oauth2/v3/userinfo'
    }, (accessToken, refreshToken, profile, done) => {
        const user = {
            googleId: profile.id,
            email: profile.emails[0].value,
            name: profile.displayName,
            picture: profile.photos?.[0]?.value,
            domain: profile.__json?.hd
        };
        // TODO: check user against allow list in database
        return done(null, user);
    }
    ));

    passport.serializeUser((user, done) => {
        done(null, user);
    });
    
    passport.deserializeUser((user, done) => {
        done(null, user);
    });

    app.use(passport.initialize());
    app.use(passport.session());
}

export function setupAuthRoutes(app, sendPage, __dirname) {
    app.get('/login', passport.authenticate('google', { scope: ['profile', 'email'] }));

    app.get('/auth/google/callback', 
        passport.authenticate('google', { failureRedirect: '/login-failed' }),
        (req, res) => {
            // Successful authentication, redirect home.
            res.redirect('/');
        }
    );

    app.get('/login-failed', (req, res) => {
        res.status(401).sendPage(res, path.join(__dirname, 'views', 'login-failed.html'));
    });

    app.get('/logout', (req, res) => {
        req.logout((err) => {
            if (err) return next(err);
            res.redirect('/');
        });
    });
}