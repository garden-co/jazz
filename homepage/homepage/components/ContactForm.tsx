'use client';

import { useState } from 'react';
import { Input, Button, Label } from 'quint-ui';

interface FormData {
    appName: string;
    description: string;
    projectUrl: string;
    repo: string;
    preferredCommunication: string;
    handle: string;
    message: string;
    nickName?: string; // bot protection, hidden, should be left empty by actual user
  }

  interface FormErrors {
    appName?: string;
    handle?: string;
    description?: string;
  }

const defaultFormData: FormData = {
  appName: '',
  description: '',
  projectUrl: '',
  repo: '',
  preferredCommunication: 'email',
  handle: '',
  message: ''
};

function FieldError({ message }: { message?: string }) {
    return message ? <p className="text-sm text-red-600 mt-1">{message}</p> : null;
  }

export function ContactForm() {
  const [formData, setFormData] = useState<FormData>(defaultFormData);
  const [errors, setErrors] = useState<FormErrors>({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitStatus, setSubmitStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [submitMessage, setSubmitMessage] = useState('');

  const validateForm = (): boolean => {
    const newErrors: FormErrors = {};
  
    if (!formData.appName.trim()) {
      newErrors.appName = 'App name is required';
    }
  
    if (!formData.handle.trim()) {
      newErrors.handle = 'Method of communication is required';
    } else if (
      formData.preferredCommunication === 'email' &&
      !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.handle)
    ) {
      newErrors.handle = 'Please enter a valid email address';
    }
  
    if (!formData.description.trim()) {
      newErrors.description = 'Description is required';
    }
  
    // Don't validate nickName, it's just the spam trap
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!validateForm()) {
      return;
    }

    setIsSubmitting(true);
    setSubmitStatus('idle');
    const trimmedData: FormData = {
        ...formData,
        appName: formData.appName.trim(),
        description: formData.description.trim(),
        handle: formData.handle.trim(),
        projectUrl: formData.projectUrl.trim(),
        repo: formData.repo.trim(),
        message: formData.message.trim(),
      };

    try {
      const response = await fetch('/api/contact', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(trimmedData),
      });

      const result = await response.json();

      if (response.ok) {
        setSubmitStatus('success');
        setSubmitMessage(result.message);
        // Reset form on success
        setFormData(defaultFormData);
      } else {
        setSubmitStatus('error');
        setSubmitMessage(result.error || 'Something went wrong. Please try again.');
      }
    } catch (error) {
      setSubmitStatus('error');
      setSubmitMessage('Network error. Please check your connection and try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleInputChange = (field: keyof FormData, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
    // Clear error when user starts typing
    if (errors[field as keyof FormErrors]) {
      setErrors(prev => ({ ...prev, [field]: undefined }));
    }
  };

  return (
    <div className="max-w-2xl mx-auto p-6 bg-white rounded-lg shadow-lg dark:bg-stone-900 dark:text-white">
      <div className="text-center mb-8">
        <h2 className="text-3xl font-bold text-stone-900 dark:text-white mb-2">
          Submit a Project
        </h2>
        <p className="text-stone-600 dark:text-stone-400">
          We'd love to hear more about your jazz app. Please fill out the form below and we'll get back to you as soon as possible.
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 ">
          <div>
            <Label htmlFor="appName" size="md">
              App Name *
            </Label>
            <Input
              id="appName"
              type="text"
              value={formData.appName}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleInputChange('appName', e.target.value)}
              intent={errors.appName ? 'danger' : 'default'}
              sizeStyle="md"
              placeholder="The name of your app"
            />
            {errors.appName && <FieldError message={errors.appName} />}
          </div>
          <div>
            <Label htmlFor="description" size="md">
              Description *
            </Label>
            <Input
              id="description"
              type="text"
              value={formData.description}
              intent={errors.description ? 'danger' : 'default'}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleInputChange('description', e.target.value)}
              sizeStyle="md"
              placeholder="Brief description of your app"
            />
            {errors.description && <FieldError message={errors.description} />}
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div>
            <Label htmlFor="contactMethod" size="md">
              Preferred Contact Method *
            </Label>
            <select
              id="contactMethod"
              value={formData.preferredCommunication}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => handleInputChange('preferredCommunication', e.target.value)}
              className="w-full rounded-md border pl-3.5 text-base px-2.5 py-1 shadow-sm h-[36px] font-medium dark:text-white border-stone-500/50"
            >
              <option value="email">Email</option>
              <option value="discord">Discord</option>
            </select>
          </div>
          <div>
            <Label htmlFor="handle" size="md">
              Email/Discord Handle *
            </Label>
            <Input
              id="handle"
              type={formData.preferredCommunication === "email" ? "email" : "text"}
              value={formData.handle}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleInputChange('handle', e.target.value)}
              intent={errors.handle ? 'danger' : 'default'}
              sizeStyle="md"
              placeholder={formData.preferredCommunication === "email" ? "your.email@example.com" : "your.discord.handle"}
            />
            {errors.handle && <FieldError message={errors.handle} />}
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <Label htmlFor="projectUrl" size="md">
              Project URL
            </Label>
            <Input
              id="projectUrl"
              type="text"
              value={formData.projectUrl}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleInputChange('projectUrl', e.target.value)}
              sizeStyle="md"
              placeholder="Your project url"
            />
          </div>
          <div>
            <Label htmlFor="repo" size="md">
              Project Repository (optional)
            </Label>
            <Input
              id="repo"
              type="text"
              value={formData.repo}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleInputChange('repo', e.target.value)}
              sizeStyle="md"
              placeholder="Your project repo"
            />
          </div>
        </div>
        <div className="grid grid-cols-1 gap-6">
        <div>
          <Label htmlFor="message" size="md">
            Message (optional)
          </Label>
          <textarea
            id="message"
            value={formData.message}
            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => handleInputChange('message', e.target.value)}
            className="w-full rounded-md border py-1.5 px-3 h-24 text-base font-medium dark:text-white border-stone-500/50 focus:ring-stone-800/50 focus:outline-none focus:ring-2"
            placeholder="Anything else you'd like to add?"
            autoComplete="off"
            tabIndex={-1}
            rows={5}
          />
        </div>
        {/* this is a bot protection field, hidden from the user */}
        <div className="hidden" aria-hidden="true">
            <label htmlFor="nickName">Last Name</label>
            <input
                id="nickName"
                name="nickName"
                type="text"
                tabIndex={-1}
                autoComplete="off"
                value={formData.nickName || ""}
                onChange={(e) => handleInputChange("nickName" as keyof FormData, e.target.value)}
            />
        </div>
        </div>
        

        {submitStatus !== 'idle' && (
          <div className={`p-4 rounded-md ${
            submitStatus === 'success' 
              ? 'bg-green-50 text-green-800 border border-green-200 dark:bg-green-900/20 dark:text-green-400 dark:border-green-800' 
              : 'bg-red-50 text-red-800 border border-red-200 dark:bg-red-900/20 dark:text-red-400 dark:border-red-800'
          }`}>
            {submitMessage}
          </div>
        )}

        <div className="text-center">
          <Button
            type="submit"
            disabled={isSubmitting}
            variant="outline"
            intent="primary"
            size="lg"
            className="w-full md:w-auto px-8"
          >
            {isSubmitting ? 'Sending...' : 'Submit'}
          </Button>
        </div>
      </form>
    </div>
  );
}
