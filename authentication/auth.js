import fs from "node:fs";
import path from 'path';
import { fileURLToPath } from 'node:url';
import 'dotenv/config';
import session from 'express-session';
import Database from 'better-sqlite3';
import betterSqlite3SessionStoreFactory from 'better-sqlite3-session-store';
import passport from 'passport';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';
import { eq } from 'drizzle-orm';
import { db } from '../database/index.js';
import { allowedEmails } from '../database/schema.js';

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

function seedAdminEmails() {
    const raw = process.env.ADMIN_EMAILS || '';
    const emails = raw
        .split(',')
        .map(e => e.trim().toLowerCase())
        .filter(Boolean);

    for (const email of emails) {
        db.insert(allowedEmails)
            .values({ email, note: 'admin (seeded from ADMIN_EMAILS)' })
            .onConflictDoNothing()
            .run();
    }

    if (emails.length) {
        console.log(`Seeded ${emails.length} admin email(s) into allow list.`);
    }
}

export function setupPassport(app) {
    seedAdminEmails();

    passport.use(new GoogleStrategy({
        clientID: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        callbackURL: '/auth/google/callback',
        userProfileURL: 'https://www.googleapis.com/oauth2/v3/userinfo'
    }, (accessToken, refreshToken, profile, done) => {
        const email = profile.emails?.[0]?.value?.toLowerCase();
        if (!email) {
            return done(null, false, { message: 'No email on Google profile' });
        }

        const allowed = db
            .select()
            .from(allowedEmails)
            .where(eq(allowedEmails.email, email))
            .get();

        if (!allowed) {
            return done(null, false, { message: 'Email not on allow list' });
        }

        const user = {
            googleId: profile.id,
            email,
            name: profile.displayName,
            picture: profile.photos?.[0]?.value,
            domain: profile.__json?.hd
        };
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
    app.get('/login', (req, res, next) => {
        req.session.returnTo = req.headers.referer || '/';
        next();
    }, passport.authenticate('google', { scope: ['profile', 'email'] }));

    app.get('/auth/google/callback', 
        passport.authenticate('google', { failureRedirect: '/login-failed' }),
        (req, res) => {
            // Successful authentication, redirect home.
            const returnTo = req.session.returnTo || '/'
            delete req.session.returnTo;

            req.session.save((err) => {
                if (err) console.error('Session save error:', err);
                res.redirect(returnTo);
            });
        }
    );

    app.get('/login-failed', (req, res) => {
        res.status(401).sendPage(res, path.join(__dirname, 'views', 'login-failed.html'));
    });

    app.get('/logout', (req, res, next) => {
        req.logout((err) => {
            if (err) return next(err);
            res.redirect('/');
        });
    });
}