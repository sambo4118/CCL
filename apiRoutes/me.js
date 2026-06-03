import express from 'express';

const router = express.Router();

router.get('/', (req, res) => {
    if (req.isAuthenticated()) {
        res.json({
            loggedIn: true,
            user: {
                email: req.user.email,
                name: req.user.name,
                picture: req.user.picture,
                domain: req.user.domain
            }
        });
    } else {
        res.json({ loggedIn: false });
    }
});

export default router;
