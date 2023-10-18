import React, { useState } from 'react';
import './signInForm.css'

function Login() {
  return (
    <div className="login-container">
        <div className="login-box">
            <h2>Connexion</h2>
            <form>
                <div className="input-container">
                    <input type="email" placeholder="Adresse e-mail" required />
                </div>
                <div className="input-container">
                    <input type="password" placeholder="Mot de passe" required />
                </div>
                <a href="#" className="forgot-password">Mot de passe oublié ?</a>
                <button className="login-button">Me connecter</button>
            </form>
        </div>
    </div>
);
}

export default Login;